<?php
/**
 * Created by Ivanoff.
 * User: Ivanoff
 * Date: 01.04.2017
 * Time: 14:14
 */

namespace ivanoff\resque;


use yii\base\Component;

abstract class Job extends Component
{
    public $id;

    public $queue;

    public $args;


    abstract public function perform();

    final public function updateStatus($status)
    {
        if(empty($this->id)) {
            return;
        }
        $jobStatus = new JobStatus([
            'id' => $this->id,
        ]);
        $jobStatus->update($status);
    }
}
