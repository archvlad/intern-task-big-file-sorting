const { createReadStream, createWriteStream } = require('fs');
const { rm, rename } = require('fs/promises');
const { pipeline } = require('stream/promises');
const readline = require('readline');
const crypto = require('crypto');

const BUFFER_CAPACITY = 100000;
const MAX_MEM_USE = 10000000;
const FILE_NAME = 'data.csv';

async function merge(filesToMerge, mergedFileName) {
  console.log('merging files', filesToMerge, 'to one file', mergedFileName);
  // const resultFileName = `${fileName.split('.txt')[0]}-sorted.txt`;
  // const resultFileName = mergedFileName;
  const file = createWriteStream(mergedFileName, {
    highWaterMark: BUFFER_CAPACITY,
  });
  const activeReaders = filesToMerge.map((name) => readline
    .createInterface({
      input: createReadStream(name, {
        highWaterMark: BUFFER_CAPACITY,
      }),
      crlfDelay: Infinity,
    })
    [Symbol.asyncIterator]());
  console.log('get activeReaders');
  const values = await Promise.all(
    activeReaders.map((r) => r.next().then((e) => e.value)),
  );
  return pipeline(async function* () {
    while (activeReaders.length > 0) {
      const sortedValues = [...values].sort((a, b) => {
        const dateA = new Date(a.split(',')[1]);
        const dateB = new Date(b.split(',')[1]);
        return Math.sign(dateA - dateB);
      });
      const sortedItem = sortedValues[0];
      const sortedItemIndex = values.indexOf(sortedItem);
      // console.log('saving', `[${sortedItemIndex}]`, sortedItem);
      yield `${sortedItem}\n`;
      const res = await activeReaders[sortedItemIndex].next();
      if (!res.done) {
        values[sortedItemIndex] = res.value;
      } else {
        values.splice(sortedItemIndex, 1);
        activeReaders.splice(sortedItemIndex, 1);
      }
    }
  }, file);
}

async function merge2(tmpFileNames, fileName) {
  const fileIndex = 0;
  while (tmpFileNames.length > 1) {
    const mergedFileName = `tmp_sort_${crypto.randomUUID()}`;
    await merge(
      [tmpFileNames[fileIndex], tmpFileNames[fileIndex + 1]],
      mergedFileName,
    );
    rm(tmpFileNames[fileIndex]);
    rm(tmpFileNames[fileIndex + 1]);
    tmpFileNames.splice(tmpFileNames.indexOf(tmpFileNames[fileIndex]), 2);
    tmpFileNames.push(mergedFileName);
  }
  rename(
    tmpFileNames[0],
    `${fileName.split('.')[0]}-sorted.${fileName.split('.')[1]}`,
  );
}

async function sortAndWriteToFile(tempChunk, tmpFileNames) {
  tempChunk.sort((a, b) => {
    const dateA = new Date(a.split(',')[1]);
    const dateB = new Date(b.split(',')[1]);
    return Math.sign(dateA - dateB);
  });
  console.log('sorted');

  const tmpFileName = `tmp_sort_${crypto.randomUUID()}`;
  tmpFileNames.push(tmpFileName);
  console.log(`creating tmp file: ${tmpFileName}`);
  const wr = createWriteStream(tmpFileName, { highWaterMark: BUFFER_CAPACITY });
  console.log('created');
  const a = tempChunk.map((e) => `${e}\n`);
  console.log('mapped');
  await pipeline(a, wr);
  wr.destroy();
  tempChunk.length = 0;
  console.log('written');
}

async function externSort(fileName) {
  const file = createReadStream(fileName, { highWaterMark: BUFFER_CAPACITY });

  const lines = readline.createInterface({
    input: file,
    crlfDelay: Infinity,
  });

  const tempChunk = [];
  let size = 0;

  const tmpFileNames = [];

  for await (const line of lines) {
    size += line.length;
    tempChunk.push(line);
    if (size > MAX_MEM_USE) {
      console.log('sorting', tempChunk.length, 'elements', size, 'bytes');
      await sortAndWriteToFile(tempChunk, tmpFileNames);
      size = 0;
    }
  }

  if (tempChunk.length > 0) {
    await sortAndWriteToFile(tempChunk, tmpFileNames);
  }
  await merge2(tmpFileNames, fileName);
  // await cleanUp(tmpFileNames);
}

externSort(FILE_NAME);
