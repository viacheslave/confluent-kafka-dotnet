using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.AccessViolation;

public class Program
{
  public static async Task Main(string[] args)
  {
    var task = await Task.Factory.StartNew(async () => await StartConsumer(), TaskCreationOptions.LongRunning);
    Console.ReadKey();
  }

  public static async Task StartConsumer()
  {
    // !!!
    // Set your servers and topic
    var bootstrapServers = "plaintext://localhost:9092";
    var topic = "events";

    var groupId = "confluent.kafka.accessViolation";

    var consumerConfig = new ConsumerConfig
    {
      BootstrapServers = bootstrapServers,
      GroupId = groupId,

      PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
      AutoOffsetReset = AutoOffsetReset.Earliest,
      EnableAutoCommit = false,
      EnablePartitionEof = true,

      MaxPollIntervalMs = 10_000,
      SessionTimeoutMs = 10_000,
      HeartbeatIntervalMs = 2_000,
    };

    IConsumer<string, string> consumer = null;

    try
    {
      consumer = new ConsumerBuilder<string, string>(consumerConfig)
        .SetLogHandler(ConsumerEvents.LogHandler)
        .SetErrorHandler(ConsumerEvents.ErrorHandler)
        .SetPartitionsAssignedHandler(ConsumerEvents.HandlePartitionsAssigned)
        .SetPartitionsRevokedHandler(ConsumerEvents.HandlePartitionsRevoked)
        .SetPartitionsLostHandler(ConsumerEvents.HandlePartitionsLost)
        //.SetStatisticsHandler(StatisticsHandlerInternal)
        .SetOffsetsCommittedHandler(ConsumerEvents.HandleOffsetsCommitted)
        .Build();

      consumer.Subscribe(topic);
    }
    catch (Exception ex)
    {
      Console.WriteLine($"Unable to subscribe, {ex.Message}");
    }

    var monitoringTask = Task.Factory.StartNew(async () => await Monitor(consumer), TaskCreationOptions.LongRunning);

    var offsets = new List<TopicPartitionOffset>();

    while (true)
    {
      try
      {
        var result = consumer.Consume();
        Console.WriteLine($"Received at offset: {result.Offset}");

        offsets.Add(result.TopicPartitionOffset);
        if (offsets.Count == 10)
        {
          consumer.Commit(offsets);
          Console.WriteLine($"Committed {offsets.Count} offsets");

          offsets.Clear();
        }

        await Task.Delay(TimeSpan.FromSeconds(20));

      }
      catch (Exception ex)
      {
        Console.WriteLine($"Consumer failed, {ex.Message}");
      }
    }
  }

  private static async Task Monitor(IConsumer<string, string> consumer)
  {
    while (true)
    {
      await Task.Delay(2_000);

      try
      {
        var task = GetLivenessTask(consumer);
        var alive = await task.TimeoutAfter(TimeSpan.FromMilliseconds(2_000));
        if (!alive)
        {
          Console.WriteLine($"Liveness: consumer is dead");

          consumer.Unsubscribe();
          consumer.Dispose();
          consumer.Close();
          break;
        }

        Console.WriteLine($"Liveness: consumer is alive");
      }
      catch (Exception ex)
      {
        Console.WriteLine($"Liveness: consumer liveness check error, {ex.Message}");
      }
    }
  }

  private static Task<bool> GetLivenessTask(IConsumer<string, string> consumer)
  {
    return Task.Factory
      .StartNew(() => IsAlive(consumer));
  }

  private static bool IsAlive(IConsumer<string, string> consumer)
  {
    return consumer?.Assignment?.Count > 0;
  }
}
