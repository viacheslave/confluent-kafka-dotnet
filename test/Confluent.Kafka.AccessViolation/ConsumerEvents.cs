using System;
using System.Collections.Generic;
using System.Threading;

namespace Confluent.Kafka.AccessViolation;

public static class ConsumerEvents
{
  internal static void HandlePartitionsRevoked(IConsumer<string, string> consumer, List<TopicPartitionOffset> partitionOffsets)
  {
    Console.WriteLine($"[{consumer.MemberId}] Partitions revoked, Count={partitionOffsets.Count}");
    Thread.Sleep(2000);
  }

  internal static void HandlePartitionsLost(IConsumer<string, string> consumer, List<TopicPartitionOffset> partitionOffsets)
  {
    Console.WriteLine($"[{consumer.MemberId}] Partitions lost, Count={partitionOffsets.Count}");
  }

  internal static void HandlePartitionsAssigned(IConsumer<string, string> consumer, List<TopicPartition> partitions)
  {
    Console.WriteLine($"[{consumer.MemberId}] Partitions assigned, Count={partitions.Count}");
  }

  internal static void HandleOffsetsCommitted(IConsumer<string, string> consumer, CommittedOffsets offsets)
  {
    Console.WriteLine($"[{consumer.MemberId}] Offsets committed, Count={offsets.Offsets.Count} {offsets.Error?.Reason} {offsets.Error?.Code}");
  }

  internal static void ErrorHandler(IConsumer<string, string> consumer, Error error)
  {
    Console.WriteLine($"[{consumer.MemberId}] Consumer error: {error}");
  }

  internal static void LogHandler(IConsumer<string, string> consumer, LogMessage logMessage)
  {
    Console.WriteLine(
      $"[{consumer.MemberId}] " +
      $"Log: {logMessage.Message}, " +
      $"Level: {logMessage.Level}, " +
      $"Client: {logMessage.Name}, " +
      $"Code facility: {logMessage.Facility}");
  }
}
