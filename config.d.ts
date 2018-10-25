export const esHost: string;
export const esIndex: string;
export const esType: string;
export const esTemplate: {
  name: string;
  body: {
    index_patterns: string[];
    mappings: any;
    settings?: {
      number_of_shards: number;
      number_of_replicas?: number;
    }
  }
}
