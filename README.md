# Baby Names!

Fun with the Social Security Administration's baby name data

## Config

Sample `config.ts`:
```javascript
export const esHost = 'http://elastic:changeme@localhost:9200';
export const esIndex = 'babynames';
export const esType = '_doc';
```

## Data 

First, you need to get the raw data from the [Social Security Administration](http://www.ssa.gov/OACT/babynames/).  Acquire the "flat" baby name data from [https://github.com/TimeMagazine/babynames](github.com/TimeMagazine/babynames). Place the "individuals" file in a directory called "data".
