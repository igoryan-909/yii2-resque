Redis queue for Yii2
====================
Simplified php-resque component for Yii2.

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
php composer.phar require --prefer-dist igoryan-909/yii2-resque "*"
```

or add

```
"igoryan-909/yii2-resque": "*"
```

to the require section of your `composer.json` file.


Usage
-----

Set config :

```php
'controllerMap' => [
    'resque' => [
        'class' => 'ivanoff\resque\controllers\ResqueController',
    ],
],
```

Start daemon :

`php yii resque/listen queue_name`

Add job class :

```php
class MyJob extends Job
{
    public function setUp()
    {
        # Set up environment for this job
    }
    
    public function perform()
    {
        # The arguments which given in resque enqueue method are available in $this->args
    }
    
    public function tearDown()
    {
        # Remove environment for this job
    }
}
```

For adding job :

```php
$resque = new Resque([
    'redis' => new Connection([
        'hostname' => 'xxx.xxx.xxx.xxx',
    ]),
]);

$token = $resque->enqueue('queue_name', Job::className(), ['arg' => 'val'], true);
```
For check job status :

```php
$status = (new JobStatus([
    'redis' => new Connection([
       'hostname' => 'xxx.xxx.xxx.xxx',
    ]),
    'id' => $token
]))->get();
```
or get the status from the resque object with its redis :

```php
$resque->status($token)->get()
```