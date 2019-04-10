import * as _cliProgress from 'cli-progress';
import * as config from '../config';
import * as fs from 'fs';
import * as moment from 'moment';
import { Client } from 'elasticsearch';
import { flatten } from 'lodash';
import { promisify } from 'util';

const PARTITION_SIZE = 500;
const UPLOADS_EXPECTED = 3849;

interface BulkResult {
  took: number;
  errors: boolean;
  items: {}[];
}

interface BabyNameDoc {
  gender: string;
  name: string;
  percent: string;
  value: number;
  year: number;
  date: Date;
}

async function runBulkPartition(
  docs: BabyNameDoc[],
  client: Client
): Promise<{ took: number; errors: boolean; items: number }> {
  const body = docs.map(
    (babyNameDoc: BabyNameDoc): [{}, BabyNameDoc] => {
      const id = `${babyNameDoc.year}-${babyNameDoc.name}-${babyNameDoc.gender}`;
      return [{ index: { _index: config.esIndex, _id: id } }, babyNameDoc];
    }
  );

  return client.bulk({ body: flatten(body) }).then(
    (result: BulkResult): { took: number; errors: boolean; items: number } => {
      return { took: result.took, errors: result.errors, items: result.items.length };
    }
  );
}

async function runDocs(
  fileSet: string[],
  client: Client
): Promise<{ files: number; uploads: number; docs: number }> {
  let files = 0;
  let uploads = 0;
  let docs = 0;

  const bar1 = new _cliProgress.Bar({}, _cliProgress.Presets.shades_classic);
  bar1.start(UPLOADS_EXPECTED, 0);

  const readFileAsync = promisify(fs.readFile);
  const fileOpenAsync = promisify(fs.open);
  const fileWriteSync = promisify(fs.write);
  const fileCloseAsync = promisify(fs.close);

  const nameDocs: BabyNameDoc[] = [];

  const upload = async (): Promise<void> => {
    try {
      const { took, errors, items } = await runBulkPartition(nameDocs, client);
      if (!errors) {
        uploads++;
        bar1.update(uploads);
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
        const yearInt = parseInt(year, 10);
        const yearConversionOffset = yearInt - 1970;
        const doc = {
          date: moment
            .utc(0)
            .add(yearConversionOffset, 'years')
            .toDate(),
          gender: babyName.gender as string,
          name: babyName.name as string,
          percent: percents[year] as string,
          value: parseInt(values[year], 10) as number,
          year: yearInt,
        };

        nameDocs.push(doc);
        docs++;

        // upload
        if (nameDocs.length === PARTITION_SIZE) {
          await upload();
          nameDocs.splice(0, PARTITION_SIZE);
        }
      }
    }
  }

  // capture remainder name docs
  await upload();

  // clean up
  bar1.stop();

  return { files, uploads, docs };
}

export async function run(client: Client): Promise<void> {
  const readdirAsync = promisify(fs.readdir);
  const fileSet = await readdirAsync('./data', { encoding: 'utf8' });
  const { files, uploads, docs } = await runDocs(fileSet, client);
  console.info('- Done! -');
  console.info(`Files found: ${fileSet.length}`);
  console.info(`Files processed: ${files}`);
  console.info(`Uploads performed: ${uploads}`);
  console.info(`Total documents: ${docs}`);
}

export {};
