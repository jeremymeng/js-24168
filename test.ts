import {
  ProcessErrorArgs,
  ServiceBusClient,
  ServiceBusMessage,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from '@azure/service-bus';
import Long from 'long';
import * as dotenv from "dotenv";
dotenv.config();

export class QueueService {

  private sbClient: ServiceBusClient;
  private sender: ServiceBusSender;
  private receiver: ServiceBusReceiver;

  constructor() {
    this.sbClient = new ServiceBusClient(process.env.SERVICEBUS_CONNECTION_STRING || "[ConnectionString]");
    this.sender = this.sbClient.createSender(process.env.QUEUE_NAME || "[QueueName]");
    this.receiver = this.sbClient.createReceiver(process.env.QUEUE_NAME || "[QueueName]");
  }

  async sendMessage(body: any, scheduledEnqueueTimeUtc: Date): Promise<string> {
    const message: ServiceBusMessage = {
      body: body,
    };
    const results = await this.sender.scheduleMessages(message, scheduledEnqueueTimeUtc);

    return results[0].toString();
  }

  async cancelMessage(messageId: string) {
    this.sender.cancelScheduledMessages(Long.fromString(messageId));
  }

  async receiveMessages(
    processMessage: (message: { id: string; content: string }) => Promise<void>,
    processError: () => Promise<void>,
  ) {
    const process = async (brokeredMessage: ServiceBusReceivedMessage) => {
      await processMessage(brokeredMessage.body);
      await this.receiver.completeMessage(brokeredMessage);
    };
    const error = async (args: ProcessErrorArgs) => {
      await processError();
    };

    this.receiver.subscribe(
      {
        processMessage: process,
        processError: error,
      },
      {
        autoCompleteMessages: false,
      },
    );
  }
}

async function main() {
  const service = new QueueService();
  const now = new Date();
  const msgId = await service.sendMessage(
    { body: "hello at" + now.toUTCString() },
    new Date(now.getTime() + 60 * 10 * 1000)
  );
  await service.cancelMessage(msgId);
}

main().catch(console.error);
