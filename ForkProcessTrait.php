<?php
/**
 * Author: Igor Ivanoff
 * Date: 03.08.2015
 * Time: 12:33
 */

declare(ticks=1);

namespace ivanoff\resque;


use Yii;
use Closure;
use yii\base\Exception;

/**
 * Trait, реализующий общие методы для работы с ветвлением процессов.
 * @package common\modules\parser\components\traits
 */
trait ForkProcessTrait
{
    /**
     * @var integer[] Id's процессов, которые работают в данный момент.
     */
    public $childProcesses = [];
    /**
     * @var callable
     */
    public $onExit;


    /**
     * Разделение на дочерние процессы.
     * Запускает пользовательскую функцию в дочернем процессе.
     * @param Closure $closure Пользовательская функция.
     * Может содержать любую логику, которая должна выполняться в дочернем процессе.
     * @throws Exception
     */
    protected function fork(Closure $closure)
    {
        $pid = pcntl_fork();

        if ($pid == -1) {
            throw new Exception('Error: pcntl_fork.');
        } elseif ($pid) {
            $this->childProcesses[] = $pid;
        } else {
            call_user_func($closure);
            exit;
        }
    }

    /**
     * Все ли процессы отработали.
     * @return boolean Все ли процессы отработали.
     */
    protected function completed()
    {
        while (count($this->childProcesses) > 0) {
            $this->checkEachProcess();
            usleep(10000);
        }
        return true;
    }

    /**
     * Имеются ли дочерние процессы.
     * @param boolean $checkEachProcess Проверить ли дочерние процессы перед вычислением.
     * @return boolean Имеются ли на данный момент дочерние процессы.
     */
    protected function hasChildren($checkEachProcess = true)
    {
        if ($checkEachProcess) {
            $this->checkEachProcess();
        }
        return count($this->childProcesses) > 0;
    }

    /**
     * Проверить каждый дочерний процесс.
     * Очистить элемент массива [[childProcesses]], если процесс отработал.
     */
    protected function checkEachProcess()
    {
        foreach ($this->childProcesses as $key => $pid) {
            $res = pcntl_waitpid($pid, $status, WNOHANG);

            // Если процесс был завершен
            if($res == -1 || $res > 0) {
                unset($this->childProcesses[$key]);
            }
            pcntl_wait($status, WNOHANG);

            if (is_callable($this->onExit)) {
                $exitStatus = pcntl_wexitstatus($status);
                call_user_func($this->onExit, $exitStatus);
            }
        }
    }

    /**
     * Убить процесс и все его дочерние процессы.
     * Убивает процесс и все его дочерние процессы, включая все уровни вложенности.
     * @param integer $parentPid Основной процесс.
     * @param null|array $pids Массив текущих процессов.
     * Из этого массивы вычисляются дочерние процессы.
     */
    protected function killProcessWithChildren($parentPid, $pids = null)
    {
        if (is_null($pids)) {
            $pids = [];
            exec('ps -A -o pid,ppid,command', $cmdOutput);
            foreach ($cmdOutput as $key => $line) {
                $pids[] = explode(' ', trim($line), 3);
            }
        }
        posix_kill($parentPid, SIGKILL);
        foreach ($pids as $pid) {
            if ($pid[1] == $parentPid) {
                $this->killProcessWithChildren($pid[0], $pids);
            }
        }
    }
}
