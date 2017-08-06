<?php

namespace adapter;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class QueueAdapter
{
    /**
     * @var AMQPConnection
     */
    private $connect;

    public function __construct($connect)
    {
        $this->connect = $connect;
        $this->channel = $this->connect->channel();
    }

    private function loadChannel($channelName)
    {
        $this->channel->queue_declare(
            $channelName,    #queue name - Имя очереди может содержать до 255 байт UTF-8 символов
            false,        #passive - может использоваться для проверки того, инициирован ли обмен, без того, чтобы изменять состояние сервера
            true,        #durable - убедимся, что RabbitMQ никогда не потеряет очередь при падении - очередь переживёт перезагрузку брокера
            false,        #exclusive - используется только одним соединением, и очередь будет удалена при закрытии соединения
            false        #autodelete - очередь удаляется, когда отписывается последний подписчик
        );
    }

    /**
     * Добавить в очередь
     *
     * @param string $channel
     * @param $data
     */
    public function put(string $channel, $data)
    {
        $this->loadChannel($channel);
        if (is_array($data)) {
            $data = 'a' . json_encode($data);
        } elseif (is_object($data)) {
            $data = 'o' . serialize($data);
        } elseif (is_string($data)) {
            $data = 's' . $data;
        } else {
            $data = 'n' . $data;
        }
        $msg = new AMQPMessage($data, ["delivery_mode" => 2]);
        $this->channel->basic_publish(
            $msg,        #message
            '',            #exchange
            $channel    #routing key
        );
    }

    /**
     * Доставь одно значение из очереди
     *
     * @param string $channel
     * @return null
     */
    public function pullOne(string $channel)
    {
        $this->loadChannel($channel);
        $while = true;
        $result = null;
        $count = 0;
        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume(
            $channel,                    #очередь
            '',                            #тег получателя - Идентификатор получателя, валидный в пределах текущего канала. Просто строка
            false,                        #не локальный - TRUE: сервер не будет отправлять сообщения соединениям, которые сам опубликовал
            false,                        #без подтверждения - отправлять соответствующее подтверждение обработчику, как только задача будет выполнена
            false,                        #эксклюзивная - к очереди можно получить доступ только в рамках текущего соединения
            false,                        #не ждать - TRUE: сервер не будет отвечать методу. Клиент не должен ждать ответа
            function ($msg) use (&$while, &$result, &$count) {
                if ($count > 0) {
                    return false;
                }
                $count++;
                $while = false;
                $data = $msg->body;
                switch (substr($data, 0, 1)) {
                    case 'a':
                        $result = json_decode(substr($data, 1), true);
                        break;
                    case 'o':
                        $result = unserialize(substr($data, 1));
                        break;
                    case 's':
                        $result = (string)(substr($data, 1));
                        break;
                    case 'n':
                        $result = (float)(substr($data, 1));
                        break;
                    default:
                        $result = null;
                        break;
                }
                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                return true;
            }
        );
        $start = microtime(true);
        try {
            while (count($this->channel->callbacks) && $while && time() - $start < 10) {
                $this->channel->wait(null, true, 2);
            }
        } catch (AMQPTimeoutException $e) {
        }
        return $result;
    }

    /**
     * Обработка очереди callback-ом
     *
     * @param string $channel
     * @param mixed $callback
     * @return null
     */
    public function pull(string $channel, $callback)
    {
        $this->loadChannel($channel);
        $this->channel->basic_consume(
            $channel,                    #очередь
            '',                            #тег получателя - Идентификатор получателя, валидный в пределах текущего канала. Просто строка
            false,                        #не локальный - TRUE: сервер не будет отправлять сообщения соединениям, которые сам опубликовал
            false,                        #без подтверждения - отправлять соответствующее подтверждение обработчику, как только задача будет выполнена
            false,                        #эксклюзивная - к очереди можно получить доступ только в рамках текущего соединения
            false,                        #не ждать - TRUE: сервер не будет отвечать методу. Клиент не должен ждать ответа
            function ($msg) use ($callback) {
                $data = $msg->body;
                switch (substr($data, 0, 1)) {
                    case 'a':
                        $result = json_decode(substr($data, 1), true);
                        break;
                    case 'o':
                        $result = unserialize(substr($data, 1));
                        break;
                    case 's':
                        $result = (string)(substr($data, 1));
                        break;
                    case 'n':
                        $result = (float)(substr($data, 1));
                        break;
                    default:
                        $result = null;
                        break;
                }
                if ($callback($result)) {
                    $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                }
                return true;
            }
        );
        try {
            while (count($this->channel->callbacks)) {
                $this->channel->wait(null, true, 0);
            }
        } catch (AMQPTimeoutException $e) {
        }
    }
}