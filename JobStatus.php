<?php
/**
 * Created by Ivanoff.
 * User: Ivanoff
 * Date: 01.04.2017
 * Time: 15:30
 */

namespace ivanoff\resque;


use yii\base\Component;
use yii\base\InvalidParamException;
use yii\helpers\Json;
use yii\redis\Connection;
use Yii;

class JobStatus extends Component
{
    const STATUS_WAITING = 1;
    const STATUS_RUNNING = 2;
    const STATUS_FAILED = 3;
    const STATUS_COMPLETE = 4;

    public $redis;

    public $id;


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
            } else {
                $this->redis = new Connection();
            }
        }
    }

    public function create($id)
    {
        $statusPacket = [
            'status' => self::STATUS_WAITING,
            'updated' => time(),
            'started' => time(),
        ];
        $this->redis->set(Resque::getNamespace() . 'job:' . $id . ':status', Json::encode($statusPacket));
    }

    public function isTracking()
    {
        if (!$this->redis->exists($this->getKey())) {
            return false;
        }

        return true;
    }

    public function update($status)
    {
        if (!$this->isTracking()) {
            return;
        }

        $statusPacket = [
            'status' => $status,
            'updated' => time(),
        ];
        $this->redis->set($this->getKey(), json_encode($statusPacket));

        // Expire the status for completed jobs after 24 hours
        if (in_array($status, $this->getCompleteStatuses())) {
            $this->redis->expire($this->getKey(), 86400);
        }
    }

    public function get()
    {
        if (!$this->isTracking()) {
            return false;
        }

        $statusPacket = Json::decode($this->redis->get($this->getKey()));
        if (!$statusPacket) {
            return false;
        }

        return $statusPacket['status'];
    }

    public function stop()
    {
        $this->redis->del($this->getKey());
    }

    public function getCompleteStatuses()
    {
        return [
            self::STATUS_FAILED,
            self::STATUS_COMPLETE
        ];
    }

    protected function getKey()
    {
        return Resque::getNamespace() . 'job:' . $this->id . ':status';
    }
}