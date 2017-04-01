<?php
/**
 * Created by Ivanoff.
 * User: Ivanoff
 * Date: 01.04.2017
 * Time: 14:00
 */

namespace ivanoff\resque\controllers;


use ivanoff\resque\ForkProcessTrait;
use ivanoff\resque\JobStatus;
use ivanoff\resque\Worker;
use yii\console\Controller;

class ResqueController extends Controller
{
    use ForkProcessTrait;

    /**
     * @var Worker
     */
    public $worker;
    /**
     * @var boolean
     */
    public $shutdown = false;
    /**
     * @var boolean
     */
    public $paused = false;


    public function actionListen($queue, $interval = 5)
    {
        $this->registerSigHandlers();
        $this->worker = new Worker([
            'queue' => $queue,
        ]);
        $this->worker->register();
        $this->worker->pruneDeadWorkers();
        $this->onExit = function ($exitStatus) {
            if ($exitStatus !== 0) {
                $this->worker->job->updateStatus(JobStatus::STATUS_FAILED);
            }
        };
        while (true) {
            if (!$this->hasChildren()) {
                if ($this->shutdown) {
                    break;
                }
                if ($this->paused) {
                    sleep($interval);
                    continue;
                }
                $this->worker->reserveJob();
                if ($this->worker->job !== false) {
                    $this->fork(function () {
                        $this->updateProcLine('is running');
                        $this->worker->perform();
                    });
                }
            }
            sleep($interval);
        }
    }

    protected function registerSigHandlers()
    {
        pcntl_signal(SIGTERM, [$this, 'shutDownNow']);
        pcntl_signal(SIGINT, [$this, 'shutDownNow']);
        pcntl_signal(SIGQUIT, [$this, 'shutdown']);
        pcntl_signal(SIGUSR1, [$this, 'killChild']);
        pcntl_signal(SIGUSR2, [$this, 'pauseProcessing']);
        pcntl_signal(SIGCONT, [$this, 'unPauseProcessing']);
    }

    protected function pauseProcessing()
    {
        $this->paused = true;
    }

    protected function unPauseProcessing()
    {
        $this->paused = false;
    }

    protected function shutdown()
    {
        $this->shutdown = true;
    }

    protected function shutdownNow()
    {
        $this->shutdown();
        $this->killChild();
    }

    protected function killChild()
    {
        if ($this->worker->job !== false) {
            $this->worker->job->updateStatus(JobStatus::STATUS_COMPLETE);
        }
        $this->worker->doneWorking();
        foreach ($this->childProcesses as $childProcess) {
            $this->killProcessWithChildren($childProcess);
            $this->checkEachProcess();
        }
    }

    private function updateProcLine($status)
    {
        $processTitle = 'resque' . ': ' . $status;
        if (function_exists('cli_set_process_title') && PHP_OS !== 'Darwin') {
            cli_set_process_title($processTitle);
        } elseif (function_exists('setproctitle')) {
            setproctitle($processTitle);
        }
    }
}
