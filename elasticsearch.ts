import { Client } from 'elasticsearch';
import * as fs from 'fs';
import { flatten } from 'lodash';
import { promisify } from 'util';
import * as config from './config';

const LOG_FILE = 'babies.log';
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
  settings: {
    'index.mapping.coerce': false,
    number_of_replicas: 0,
    number_of_shards: 2,
  },
};

const readdirAsync = promisify(fs.readdir);
const readFileAsync = promisify(fs.readFile);
const fileOpenAsync = promisify(fs.open);
const fileWriteSync = promisify(fs.write);
const fileCloseAsync = promisify(fs.close);

function getClient(): Client {
  return new Client({
    host: config.esHost,
    log: 'info',
  });
}

interface IBabyNameDoc {
  gender: string;
  name: string;
  percent: string;
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
  fileSet: string[],
  client: Client
): Promise<{ files: number; uploads: number; docs: number }> {
  let files = 0;
  let uploads = 0;
  let docs = 0;

  const fsHandle = await fileOpenAsync(LOG_FILE, 'w');
  const nameDocs: IBabyNameDoc[] = [];

  const upload = async () => {
    try {
      const { took, errors, items } = await runBulkPartition(nameDocs, client);
      await fileWriteSync(fsHandle, JSON.stringify({ took, errors, items }) + '\n');
      if (!errors) {
        uploads++;
      }
    } catch (err) {
      console.error('Bulk didnt work! ' + err.message);
    }
  };

  for (const file of fileSet) {
    files++;
    const contents = await readFileAsync('./data/' + file, { encoding: 'utf8' });
    const babyName = JSON.parse(contents);
    const { values, percents } = babyName;
    for (const year in values) {
      if (values.hasOwnProperty(year) && percents.hasOwnProperty(year)) {
        // map
        const doc = {
          gender: babyName.gender as string,
          name: babyName.name as string,
          percent: percents[year] as string,
          value: parseInt(values[year], 10) as number,
          year: parseInt(year, 10),
        };
        nameDocs.push(doc);
        docs++;

        // process
        if (nameDocs.length === PARTITION_SIZE) {
          await upload();
          nameDocs.splice(0, PARTITION_SIZE);
        }

        // log
        await fileWriteSync(fsHandle, JSON.stringify(doc) + '\n');
      }
    }
  }

  // capture remainder name docs
  await upload();

  // clean up
  await fileCloseAsync(fsHandle);

  return { files, uploads, docs };
}

async function setup(): Promise<void> {
  const client = getClient();
  try {
    await client.indices.create({
      body: INDEX_SETTINGS,
      index: config.esIndex,
    });
    console.info('Index is ok!');
    const fileSet = await readdirAsync('./data', { encoding: 'utf8' });
    console.info('Running...');
    const { files, uploads, docs } = await runDocs(fileSet, client);
    console.info('Done!');
    console.info(`Files found: ${fileSet.length}`);
    console.info(`Files processed: ${files}`);
    console.info(`Uploads performed: ${uploads}`);
    console.info(`Total documents: ${docs}`);
  } catch (err) {
    console.error('something is NOT ok!');
    console.error(err);
  }
}

setup();
