const { createReadStream, createWriteStream } = require("fs");
const { rm } = require("fs/promises");
const { pipeline } = require("stream/promises");
const readline = require("readline");

const BUFFER_CAPACITY = 1_000;
const MAX_MEM_USE = 8_000_00;
const FILE_NAME = "data.txt";

async function externSort(fileName) {
    const file = createReadStream(fileName, { highWaterMark: BUFFER_CAPACITY });
    const lines = readline.createInterface({
        input: file,
        crlfDelay: Infinity,
    });
    const v = [];
    let size = 0;
    const tmpFileNames = [];
    for await (let line of lines) {
        size += line.length;
        v.push(parseFloat(line));
        if (size > MAX_MEM_USE) {
            console.log("here", v.length, "elements", size, "bytes");
            await sortAndWriteToFile(v, tmpFileNames);
            size = 0;
        }
    }
    if (v.length > 0) {
        await sortAndWriteToFile(v, tmpFileNames);
    }
    await merge(tmpFileNames, fileName);
    await cleanUp(tmpFileNames);
}

function cleanUp(tmpFileNames) {
    return Promise.all(tmpFileNames.map((f) => rm(f)));
}

async function merge(tmpFileNames, fileName) {
    console.log("merging result ...");
    const resultFileName = `${fileName.split(".txt")[0]}-sorted.txt`;
    const file = createWriteStream(resultFileName, {
        highWaterMark: BUFFER_CAPACITY,
    });
    console.log("here");
    const activeReaders = tmpFileNames.map((name) =>
        readline
            .createInterface({
                input: createReadStream(name, {
                    highWaterMark: BUFFER_CAPACITY,
                }),
                crlfDelay: Infinity,
            })
            [Symbol.asyncIterator]()
    );
    console.log("get activeReaders");
    const values = await Promise.all(
        activeReaders.map((r) => r.next().then((e) => parseFloat(e.value)))
    );
    console.log("mapped");
    return pipeline(async function* () {
        while (activeReaders.length > 0) {
            const [minVal, i] = values.reduce(
                (prev, cur, idx) => (cur < prev[0] ? [cur, idx] : prev),
                [Infinity, -1]
            );
            yield `${minVal}\n`;
            const res = await activeReaders[i].next();
            if (!res.done) {
                values[i] = parseFloat(res.value);
            } else {
                values.splice(i, 1);
                activeReaders.splice(i, 1);
            }
        }
    }, file);
}

async function sortAndWriteToFile(v, tmpFileNames) {
    v.sort((a, b) => a - b);
    console.log("sorted");

    let tmpFileName = `tmp_sort_${tmpFileNames.length}.txt`;
    tmpFileNames.push(tmpFileName);
    console.log(`creating tmp file: ${tmpFileName}`);
    let wr = createWriteStream(tmpFileName, { highWaterMark: BUFFER_CAPACITY });
    console.log("created");
    let a = v.map((e) => `${e}\n`);
    console.log("mapped");
    await pipeline(a, wr);
    wr.destroy();
    v.length = 0;
    console.log("written");
}

externSort(FILE_NAME);
