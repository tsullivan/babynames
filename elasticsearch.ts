import { Client } from 'elasticsearch';
import { readdir, readFile } from 'fs';
import { flatten } from 'lodash';
import { promisify } from 'util';
import * as config from './config';

const PARTITION_SIZE = 500;
const TEMPLATE = {
  body: {
    index_patterns: [config.esIndex],
    mappings: {
      [config.esType]: {
        properties: {
          gender: { type: 'keyword' },
          name: { type: 'keyword' },
          percent: { type: 'float' },
          ranking: { type: 'integer' },
          year: { type: 'integer' },
        }
      }
    },
    settings: {
      number_of_replicas: 0,
      number_of_shards: 2,
    }
  },
  name: config.esIndex,
}


const readdirAsync = promisify(readdir);
const readFileAsync = promisify(readFile);

function getClient(): Client {
  return new Client({
    host: config.esHost,
    log: 'info',
  });
}

async function checkClient(client: Client): Promise<void> {
  await client.ping({ requestTimeout: 30000 });
}

interface IBabyNameDoc {
  gender: string;
  name: string;
  percent: number;
  ranking: number;
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
): Promise<void> {
  const body = docs.map(babyNameDoc => {
    const id = `${babyNameDoc.name}-${babyNameDoc.gender}-${babyNameDoc.year}`;
    return [
      { index: { _index: config.esIndex, _type: config.esType, _id: id } },
      babyNameDoc,
    ];
  });

  return client.bulk({ body: flatten(body) }).then((result: IBulkResult) => {
    console.log({ took: result.took, errors: result.errors, items: result.items.length });
  });
}

async function runDocs(files: string[], client: Client): Promise<number> {
  let idx = 0;
  const nameDocs: IBabyNameDoc[] = [];

  for (const file of files) {
    const contents = await readFileAsync('./data/' + file, { encoding: 'utf8' });
    const babyName = JSON.parse(contents);
    const { values: years, percents } = babyName;
    for (const year in years) {
      if (years.hasOwnProperty(year) && percents.hasOwnProperty(year)) {
        idx++;

        // map
        nameDocs.push({
          gender: babyName.gender as string,
          name: babyName.name as string,
          percent: parseFloat(percents[year]) as number,
          ranking: parseInt(babyName.year, 10) as number,
          year: parseInt(year, 10),
        });

        // process
        if (nameDocs.length === PARTITION_SIZE) {
          try {
            await runBulkPartition(nameDocs, client);
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

  return idx;
}

async function setup(): Promise<void> {
  const client = getClient();
  try {
    await checkClient(client);
    console.info('client is ok!');

    await client.indices.putTemplate(TEMPLATE);
    console.info('template is ok!');

    const files = await readdirAsync('./data', { encoding: 'utf8' });
    console.info(`files found: ${files.length}`);

    await runDocs(files, client);
  } catch (err) {
    console.error('something is NOT ok!');
    console.error(err);
  }
}

setup();
