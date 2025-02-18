# Sparklet

## 介绍

`Sparklet`- 这是一个保留基础功能的仿照`Spark`的本地大数据计算引擎`Demo`。

## 架构

一个标准的`reactor`模型，有一个主线程，一个`eventloop`线程，，然后将任务分发到包含`IO/数据计算`的协程/虚拟线程。

也是一个`master-worker`模型，其中`master`负责任务划分以及事件处理，`worker(backend)`负责数据任务处理。

具体设计看内部的设计文档。

## 使用

运行环境:

* Scala2.12 及以上
* JDK21 及以上

框架目前还没有进一步完善，只有少量`RDD`实现，同时并不支持`SQL`操作。

目前支持从本地文件读取数据，然后进行`map,reduceByKey`等操作，并且可以将结果收集到`master`。

比如，可以运行自带的`wordCount`实例，中间结果存储在`tmp`文件夹下。
