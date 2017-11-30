<?php

namespace bongrun\adapter;

/**
 * Interface QueueAdapterInterface
 * @package bongrun\adapter
 */
interface QueueAdapterInterface
{
    /**
     * Добавить в очередь
     *
     * @param string $channel
     * @param $data
     */
    public function put(string $channel, $data);

    /**
     * Обработка очереди callback-ом
     *
     * @param string $channel
     * @param mixed $callback
     * @return null
     */
    public function pull(string $channel, $callback);
}