# Baby Names!

Fun with the Social Security Administration's baby name data

## Config

Sample `config.ts`:
```javascript
export const esHost = 'http://elastic:changeme@localhost:9200';
export const esIndex = 'babynames';
```

## Data 

First, you need to get the raw data from the [Social Security Administration](http://www.ssa.gov/OACT/babynames/).  Acquire the "flat" baby name data from [https://github.com/TimeMagazine/babynames](github.com/TimeMagazine/babynames). Place the "individuals" file in a directory called "data".

## Logging

Activity will be written to `babies.log`. You don't want to `tail -f` this file, it will crash your terminal. Instead, do something like:
```
watch -n2 'tail -10 babies.log'
```
Just get a glimpse of what the last 10 baby names are being worked on, once every 2 seconds.
