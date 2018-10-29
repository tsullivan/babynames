import { Client } from 'elasticsearch';
import { readdir, readFile } from 'fs';
import { flatten } from 'lodash';
import { promisify } from 'util';
import * as config from './config';

const PARTITION_SIZE = 500;
const INDEX_SETTINGS = {
  mappings: {
    [config.esType]: {
      properties: {
        gender: { type: 'keyword' },
        name: { type: 'keyword' },
        percent: { type: 'float' },
        value: { type: 'integer' },
        year: { type: 'integer' },
      },
    },
  },
  settings: { number_of_replicas: 0, number_of_shards: 2 },
};

const readdirAsync = promisify(readdir);
const readFileAsync = promisify(readFile);

function getClient(): Client {
  return new Client({
    host: config.esHost,
    log: 'info',
  });
}

// async function checkClient(client: Client): Promise<void> {
//  await client.ping({ requestTimeout: 30000 });
// }

interface IBabyNameDoc {
  gender: string;
  name: string;
  percent: number;
  value: number;
  year: number;
}

interface IBulkResult {
  took: number;
  errors: boolean;
  items: any[];
}

async function runBulkPartition(
  docs: IBabyNameDoc[],
  client: Client
): Promise<{ took: number; errors: boolean; items: number }> {
  const body = docs.map(babyNameDoc => {
    const id = `${babyNameDoc.year}-${babyNameDoc.name}-${babyNameDoc.gender}`;
    return [
      { index: { _index: config.esIndex, _type: config.esType, _id: id } },
      babyNameDoc,
    ];
  });

  return client.bulk({ body: flatten(body) }).then((result: IBulkResult) => {
    return { took: result.took, errors: result.errors, items: result.items.length };
  });
}

async function runDocs(
  files: string[],
  client: Client
): Promise<{ items: number; uploads: number }> {
  let idx = 0;
  let uploads = 0;
  const nameDocs: IBabyNameDoc[] = [];

  for (const file of files) {
    const contents = await readFileAsync('./data/' + file, { encoding: 'utf8' });
    const babyName = JSON.parse(contents);
    const { values, percents } = babyName;
    for (const year in values) {
      if (values.hasOwnProperty(year) && percents.hasOwnProperty(year)) {
        idx++;

        // map
        const doc = {
          gender: babyName.gender as string,
          name: babyName.name as string,
          percent: parseFloat(percents[year]) as number,
          value: parseInt(values[year], 10) as number,
          year: parseInt(year, 10),
        };
        nameDocs.push(doc);

        // process
        if (nameDocs.length === PARTITION_SIZE) {
          try {
            const { took, errors, items } = await runBulkPartition(nameDocs, client);
            console.log({ took, errors, items });
            if (!errors) {
              uploads++;
            }
          } catch (err) {
            console.error('Bulk didnt work! ' + err.message);
          }
          nameDocs.splice(0, PARTITION_SIZE);
        }
      }
    }
  }

  // remainder
  await runBulkPartition(nameDocs, client);

  return { items: idx, uploads };
}

async function setup(): Promise<void> {
  const client = getClient();
  try {
    // await checkClient(client);
    // console.info('client is ok!');

    await client.indices.create({
      body: INDEX_SETTINGS,
      index: config.esIndex,
    });
    console.info('template is ok!');

    const files = await readdirAsync('./data', { encoding: 'utf8' });
    console.info(`files found: ${files.length}`);

    const { items, uploads } = await runDocs(files, client);
    console.log({ items, uploads });
  } catch (err) {
    console.error('something is NOT ok!');
    console.error(err);
  }
}

setup();
