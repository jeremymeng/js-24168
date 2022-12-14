import {
  ProcessErrorArgs,
  ServiceBusClient,
  ServiceBusMessage,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from '@azure/service-bus';
import Long from 'long';


export class QueueService {

  private sbClient: ServiceBusClient;
  private sender: ServiceBusSender;
  private receiver: ServiceBusReceiver;

  constructor() {
    this.sbClient = new ServiceBusClient("[ConnectionString]");
    this.sender = this.sbClient.createSender("[QueueName]");
    this.receiver = this.sbClient.createReceiver("[QueueName]");
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
