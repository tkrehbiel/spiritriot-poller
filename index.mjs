import {
  DynamoDBClient,
  BatchGetItemCommand,
  BatchWriteItemCommand,
} from '@aws-sdk/client-dynamodb';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import dotenv from 'dotenv';

dotenv.config({ path: './.env.local' });

// TODO: sanity checks on the env vars
const jsonFeedUrl = process.env.JSON_FEED_URL;
const stateTableName = process.env.STATE_TABLE_NAME;
const notifyQueueName = process.env.NOTIFY_QUEUE_NAME;
const startTriggerDate = process.env.START_TRIGGER_DATE;
const endTriggerDate = process.env.END_TRIGGER_DATE;

const dynamoClient = new DynamoDBClient({
  credentials: fromNodeProviderChain(),
});

const sqsClient = new SQSClient({
  credentials: fromNodeProviderChain(),
});

class AppError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AppError';
  }
}

async function fetchPosts(sourceUrl) {
  const posts = await fetch(sourceUrl)
    .then((response) => {
      if (response.status !== 200) {
        throw new AppError(
          `received status ${response.status} while fetching ${sourceUrl}`,
        );
      }
      return response.json();
    })
    .then((data) => {
      return data.items
        .filter(
          (item) =>
            item.date_published >= startTriggerDate &&
            item.date_published <= endTriggerDate,
        )
        .map((item) => {
          return {
            postUrl: item.url,
            postDate: item.date_published,
          };
        });
    });
  return posts;
}

async function findExistingItems(posts) {
  const batchGetItemCommand = new BatchGetItemCommand({
    RequestItems: {
      [stateTableName]: {
        Keys: posts.map((post) => ({
          url: { S: post.postUrl },
        })),
      },
    },
  });
  const data = await dynamoClient.send(batchGetItemCommand);
  if (data.Responses) {
    const items = data.Responses[stateTableName];
    return items;
  }
  return [];
}

async function detectNewPosts(posts, items) {
  const startTime = new Date(process.env.START_TRIGGER_DATE);
  const endTime = new Date(process.env.START_TRIGGER_DATE);
  return posts.filter((post) => {
    const found = items.find((item) => item.url.S === post.postUrl);
    return found === undefined;
  });
}

async function sendMessage(message) {
  const command = new SendMessageCommand({
    QueueUrl: notifyQueueName,
    MessageBody: JSON.stringify(message),
  });
  return sqsClient.send(command);
}

async function triggerNotifications(posts) {
  const promises = [];
  const newItems = [];
  for (const post of posts) {
    console.log('triggering', post.postUrl);
    const message = {
      url: post.postUrl,
      published: post.postDate,
      detected: new Date().toISOString(),
    };
    promises.push(sendMessage(message));
    newItems.push(message);
  }
  await Promise.all(promises);
  return newItems;
}

async function writeNewItems(itemsToWrite) {
  if (itemsToWrite.length === 0) {
    console.log('no new items to store');
    return;
  }
  const batchWriteCommand = new BatchWriteItemCommand({
    RequestItems: {
      [stateTableName]: itemsToWrite.map((item) => ({
        PutRequest: {
          Item: {
            url: { S: item.url },
            published: { S: item.published },
            detected: { S: item.detected },
          },
        },
      })),
    },
  });
  try {
    const data = await dynamoClient.send(batchWriteCommand);
    console.log('Successfully wrote items:', data);
  } catch (error) {
    console.error('Error writing items:', error);
  }
}

async function main() {
  try {
    // Get all posts from the source blog
    const posts = await fetchPosts(jsonFeedUrl);

    console.log('blog post feed:');
    console.log(posts);

    if (posts.length === 0) return;

    const items = await findExistingItems(posts);

    console.log('found in state table:');
    console.log(items);

    // Detect which posts are new, by comparing to the data store
    const newPosts = await detectNewPosts(posts, items);

    console.log('new, untracked posts:');
    console.log(newPosts);

    if (newPosts.length === 0) return;

    // Generate notifications for each new post,
    // returning a data store item for each one
    try {
      const newItems = await triggerNotifications(newPosts);

      console.log('new items to store:');
      console.log(newItems);

      // Store the notification data in the data store
      await writeNewItems(newItems);
    } catch (error) {
      console.log('An error occurred, items were not saved');
      throw error;
    }
  } catch (error) {
    if (error instanceof AppError) console.log(error.message);
    else throw error;
  }
}

export async function handler(event) {
  console.log('launching from handler');
  await main();
}

// Invoke main() if run directly on command line
if (import.meta.url === `file://${process.argv[1]}`) {
  console.log('launching from command line');
  (async () => await main())();
}
