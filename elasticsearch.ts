import { Client } from 'elasticsearch';
import { readdir, readFile } from 'fs';
import { flatten } from 'lodash';
import { promisify } from 'util';
import * as config from './config';

const readdirAsync = promisify(readdir);
const readFileAsync = promisify(readFile);

const PARTITION_SIZE = 500;

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
  ranking: number;
  year: number;
}

async function runBulkPartition(
  docs: IBabyNameDoc[],
  client: Client
): Promise<{ took: number; errors: boolean; items: number }> {
  const body = docs.map(babyNameDoc => {
    const id = `${babyNameDoc.name}-${babyNameDoc.gender}-${babyNameDoc.year}`;
    return [
      { index: { _index: config.esIndex, _type: config.esType, _id: id } },
      babyNameDoc,
    ];
  });

  return client.bulk({ body: flatten(body) }).then(result => {
    return { took: result.took, errors: result.errors, items: result.items.length };
  });
}

async function runDocs(files: string[], client: Client): Promise<{ items: number; uploads: number }> {
  let idx = 0;
  let uploads = 0;
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
          percent: percents[year] as number,
          ranking: years[year] as number,
          year: parseInt(year, 10),
        });

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

    await client.indices.putTemplate(config.esTemplate);
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
