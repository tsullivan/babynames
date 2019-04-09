import { Client } from 'elasticsearch';
import * as config from './config';
import { run } from './lib/run';

const INDEX_SETTINGS = {
  mappings: {
    properties: {
      date: { type: 'date' },
      gender: { type: 'keyword' },
      name: { type: 'keyword' },
      percent: { type: 'float' },
      value: { type: 'integer' },
      year: { type: 'integer' },
    },
  },
  settings: {
    'index.mapping.coerce': false,
    number_of_replicas: 0, // eslint-disable-line @typescript-eslint/camelcase
    number_of_shards: 2, // eslint-disable-line @typescript-eslint/camelcase
  },
};

async function setup(): Promise<Client> {
  const client = new Client({
    host: config.esHost,
    log: 'info',
  });
  return client.indices
    .create({
      body: INDEX_SETTINGS,
      index: config.esIndex,
    })
    .then((): Client => client);
}

setup()
  .then(run)
  .catch(
    (err): void => {
      console.error(`Setup encountered an error: ${err}`);
      console.debug(err.stack);
      process.exit();
    }
  );
