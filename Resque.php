<?php
/**
 * Created by Ivanoff.
 * User: Ivanoff
 * Date: 01.04.2017
 * Time: 14:10
 */

namespace ivanoff\resque;


use yii\base\Component;
use yii\base\InvalidParamException;
use yii\helpers\Json;
use yii\redis\Connection;

class Resque extends Component
{
    /**
     * @var string
     */
    public static $namespace = 'resque:';
    /**
     * @var null|array|Connection
     */
    public $redis = null;
    /**
     * @var array|null
     */
    public $workerServers = null;
    /**
     * @var string
     */
    public $queue = 'default';
    /**
     * @var integer|string
     */
    public $selectedServer;
    /**
     * @var string
     */
    public $connectStrategy = 'free';


    public function init()
    {
        parent::init();

        if (!$this->redis instanceof Connection) {
            if (is_array($this->redis)) {
                if (empty($this->redis['class']) || !is_string($this->redis['class'])) {
                    throw new InvalidParamException('The class param must be string.');
                } elseif (!class_exists($this->redis['class'])) {
                    throw new InvalidParamException('Invalid class.');
                }
                $this->redis = new Connection($this->redis);
            } elseif (is_array($this->workerServers)) {
                if (empty($this->workerServers)) {
                    throw new InvalidParamException('The field "workerServers" doesn\'t have any servers.');
                }
                switch ($this->connectStrategy) {
                    case 'free' :
                        $this->connectToFreeWorker($this->queue);
                        break;
                    case 'random' :
                        $this->connectToRandomWorker();
                }
            } else {
                $this->redis = new Connection();
            }
        }
    }

    /**
     * @return string
     */
    public static function getNamespace()
    {
        return static::$namespace;
    }

    /**
     * @param string $queue
     * @param string $class
     * @param array|null $args
     * @param boolean $trackStatus
     * @return string
     */
    public function enqueue($queue, $class, array $args = null, $trackStatus = false)
    {
        $id = $this->generateJobId();
        if ($args !== null && !is_array($args)) {
            throw new InvalidParamException(
                'Supplied $args must be an array.'
            );
        }
        $this->push($queue, [
            'class'	=> $class,
            'args' => [$args],
            'id' => $id,
            'queue_time' => microtime(true),
        ]);

        if ($trackStatus) {
            (new JobStatus(['redis' => $this->redis]))->create($id);
        }

        return $id;
    }

    /**
     * @param string $queue
     * @param array $items
     * @return integer
     */
    public function dequeue($queue, $items = [])
    {
        if (count($items) > 0) {
            return $this->removeItems($queue, $items);
        }

        return $this->removeList($queue);
    }

    /**
     * @param string $queue
     * @return mixed
     */
    public function waitingCount($queue)
    {
        return $this->redis->llen(static::getNamespace() . 'queue:' . $queue);
    }

    /**
     * @param string $token
     * @return boolean|integer
     */
    public function getWorkerPidByToken($token)
    {
        $workers = $this->redis->smembers(static::getNamespace() . 'workers');
        foreach ($workers as $worker) {
            $queueData = Json::decode($this->redis->get(static::getNamespace() . 'worker:' . $worker));
            if (is_array($queueData)
                && isset($queueData['payload']['id'])
                && $queueData['payload']['id'] === $token) {
                $queueParts = explode(':', $worker);
                return $queueParts[1];
            }
        }

        return false;
    }

    /**
     * @param $token
     * @return JobStatus
     */
    public function status($token)
    {
        return new JobStatus([
            'redis' => $this->redis,
            'id' => $token,
        ]);
    }

    public function connectToFreeWorker($queue)
    {
        $keys = array_keys($this->workerServers);
        shuffle($keys);
        foreach ($keys as $key) {
            try {
                $this->createConnectionByKey($key);
            } catch (\Throwable $e) {
                continue;
            }
            if ($this->waitingCount($queue) == 0) {
                $this->selectedServer = $key;
                return $key;
            }
        }

        return $this->connectToRandomWorker();
    }

    public function connectToRandomWorker()
    {
        $key = array_rand($this->workerServers);
        $this->createConnectionByKey($key);
        $this->selectedServer = $key;

        return $key;
    }

    public function push($queue, $item)
    {
        $encodedItem = Json::encode($item);
        if ($encodedItem === false) {
            return false;
        }
        $this->redis->sadd(static::getNamespace() . 'queues', $queue);
        $length = $this->redis->rpush(static::getNamespace() . 'queue:' . $queue, $encodedItem);
        if ($length < 1) {
            return false;
        }

        return true;
    }

    public function generateJobId()
    {
        return md5(uniqid('', true));
    }

    private function createConnectionByKey($key)
    {
        $this->redis = new Connection([
            'hostname' => $this->workerServers[$key]['hostname'],
            'port' => $this->workerServers[$key]['port'] ?? 6379,
            'database' => $this->workerServers[$key]['database'] ?? 0,
        ]);
    }

    private function removeItems($queue, $items = [])
    {
        $counter = 0;
        $originalQueue = 'queue:' . $queue;
        $tempQueue = $originalQueue . ':temp:'. time();
        $requeueQueue = $tempQueue . ':requeue';

        // move each item from original queue to temp queue and process it
        $finished = false;
        while (!$finished) {
            $string = $this->redis->rpoplpush(static::getNamespace() . $originalQueue, static::getNamespace() . $tempQueue);

            if (!empty($string)) {
                if ($this->matchItem($string, $items)) {
                    $this->redis->rpop(static::getNamespace() . $tempQueue);
                    $counter++;
                } else {
                    $this->redis->rpoplpush(static::getNamespace() . $tempQueue, static::getNamespace() . $requeueQueue);
                }
            } else {
                $finished = true;
            }
        }

        // move back from temp queue to original queue
        $finished = false;
        while (!$finished) {
            $string = $this->redis->rpoplpush(static::getNamespace() . $requeueQueue, static::getNamespace() . $originalQueue);
            if (empty($string)) {
                $finished = true;
            }
        }

        // remove temp queue and requeue queue
        $this->redis->del(static::getNamespace() . $requeueQueue);
        $this->redis->del(static::getNamespace() . $tempQueue);

        return $counter;
    }

    private function matchItem($string, $items)
    {
        $decoded = Json::decode($string);

        foreach($items as $key => $val) {
            # class name only  ex: item[0] = ['class']
            if (is_numeric($key)) {
                if ($decoded['class'] == $val) {
                    return true;
                }
                # class name with args , example: item[0] = ['class' => {'foo' => 1, 'bar' => 2}]
            } elseif (is_array($val)) {
                $decodedArgs = (array) $decoded['args'][0];
                if ($decoded['class'] == $key &&
                    count($decodedArgs) > 0 && count(array_diff($decodedArgs, $val)) == 0) {
                    return true;
                }
                # class name with ID, example: item[0] = ['class' => 'id']
            } else {
                if ($decoded['class'] == $key && $decoded['id'] == $val) {
                    return true;
                }
            }
        }

        return false;
    }

    private function removeList($queue)
    {
        $counter = $this->redis->llen(static::getNamespace() . 'queue:' . $queue);
        $result = $this->redis->del(static::getNamespace() . 'queue:' . $queue);
        return ($result == 1) ? $counter : 0;
    }
}
