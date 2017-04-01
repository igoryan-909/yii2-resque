<?php
/**
 * Created by Ivanoff.
 * User: Ivanoff
 * Date: 01.04.2017
 * Time: 18:19
 */

namespace ivanoff\resque;


use yii\base\Component;
use yii\base\InvalidParamException;
use yii\base\UnknownClassException;
use yii\helpers\Json;
use yii\redis\Connection;
use Yii;

class Worker extends Component
{
    /**
     * @var null|array|Connection
     */
    public $redis = null;

    public $payload = [];

    public $queue;

    public $hostname;

    public $id;
    /**
     * @var Job
     */
    public $job;


    public function init()
    {
        parent::init();

        $this->hostname = php_uname('n');
        $this->id = $this->hostname . ':'.getmypid() . ':' . $this->queue;

        if (!$this->redis instanceof Connection) {
            if (is_array($this->redis)) {
                if (empty($this->redis['class']) || !is_string($this->redis['class'])) {
                    throw new InvalidParamException('The class param must be string.');
                } elseif (!class_exists($this->redis['class'])) {
                    throw new InvalidParamException('Invalid class.');
                }
                $this->redis = new Connection($this->redis);
            } else {
                $this->redis = new Connection();
            }
        }
    }

    /**
     * @return bool|Job
     * @throws UnknownClassException
     */
    public function reserveJob()
    {
        $this->pop();
        if (!is_array($this->payload)) {
            $this->job = false;
            return $this->job;
        }
        /** @var Job $className */
        $className = $this->payload['class'];

        try {
            if (!class_exists($className)) {
                throw new UnknownClassException('Could not find job class ' . $className . '.');
            }
        } catch (\Throwable $e) {
            $this->job = false;
            return $this->job;
        }

        $this->job = new $className([
            'id' => $this->payload['id'],
            'args' => $this->getArguments(),
            'queue' => $this->queue,
        ]);

        return $this->job;
    }

    public function perform()
    {
        $this->workingOn();
        try {
            $this->job->updateStatus(JobStatus::STATUS_RUNNING);
            if (method_exists($this->job, 'setUp')) {
                $this->job->setUp();
            }
            $this->job->perform();
            if (method_exists($this->job, 'tearDown')) {
                $this->job->tearDown();
            }
            $this->job->updateStatus(JobStatus::STATUS_COMPLETE);
        } catch (\Throwable $e) {
            $this->job->updateStatus(JobStatus::STATUS_FAILED);
        }
        $this->doneWorking();
    }

    public function pop()
    {
        $item = $this->redis->lpop(Resque::getNamespace() . 'queue:' . $this->queue);
        if (!$item) {
            $this->payload = false;
        }
        $this->payload = Json::decode($item);
    }

    public function getArguments()
    {
        if (!isset($this->payload['args'])) {
            return [];
        }

        return $this->payload['args'][0];
    }

    public function pruneDeadWorkers()
    {
        $workerPids = $this->workerPids();
        $workers = $this->all();
        foreach ($workers as $worker) {
            if (is_object($worker)) {
                list($host, $pid, $queue) = explode(':', $this->id, 3);
                if ($host != $this->hostname || in_array($pid, $workerPids) || $pid == getmypid()) {
                    continue;
                }
                $worker->unregister();
            }
        }
    }

    public function workerPids()
    {
        $pids = [];
        exec('ps -A -o pid,command | grep [r]esque', $cmdOutput);
        foreach ($cmdOutput as $line) {
            list($pids[],) = explode(' ', trim($line), 2);
        }

        return $pids;
    }

    public function all()
    {
        $workers = $this->redis->smembers(Resque::getNamespace() . 'workers');
        if (!is_array($workers)) {
            $workers = [];
        }

        $instances = [];
        foreach ($workers as $workerId) {
            $instances[] = $this->find($workerId);
        }

        return $instances;
    }

    public function find($workerId)
    {
        if (!$this->exists($workerId) || false === strpos($workerId, ":")) {
            return false;
        }

        list($hostname, $pid, $queue) = explode(':', $workerId, 3);
        $worker = new self([
            'queue' => $queue,
        ]);
        $worker->setId($workerId);

        return $worker;
    }

    public function exists($workerId)
    {
        return (bool) $this->redis->sismember(Resque::getNamespace() . 'workers', $workerId);
    }

    public function register()
    {
        $this->redis->sadd(Resque::getNamespace() . 'workers', $this->id);
        $this->redis->set(Resque::getNamespace() . 'worker:' . $this->id . ':started', strftime('%a %b %d %H:%M:%S %Z %Y'));
    }

    public function unregister()
    {
        $this->redis->srem(Resque::getNamespace() . 'workers', $this->id);
        $this->redis->del(Resque::getNamespace() . 'worker:' . $this->id);
        $this->redis->del(Resque::getNamespace() . 'worker:' . $this->id . ':started');
    }

    public function workingOn()
    {
        $data = Json::encode([
            'queue' => $this->queue,
            'run_at' => strftime('%a %b %d %H:%M:%S %Z %Y'),
            'payload' => $this->payload,
        ]);
        $this->redis->set(Resque::getNamespace() . 'worker:' . $this->id, $data);
    }

    public function doneWorking()
    {
        $this->redis->del(Resque::getNamespace() . 'worker:' . $this->id);
    }

    public function setId($workerId)
    {
        $this->id = $workerId;
    }
}
