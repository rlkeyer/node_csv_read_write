const csv = require("csv-parser");
const fs = require("fs");

const groupByCarrier = (data) => {
  const carriers = {};
  data.forEach((line) => {
    line.version = parseInt(line.version);
    if (carriers[line.insurance]) {
      if (
        !carriers[line.insurance][line.userId] ||
        carriers[line.insurance][line.userId].version < line.version
      ) {
        carriers[line.insurance][line.userId] = line;
      }
    } else {
      carriers[line.insurance] = { [line.userId]: line };
    }
  });
  return carriers;
};

const sortNames = (a, b) => {
  if (a.lastName < b.lastName) {
    return -1;
  }
  if (a.lastName > b.lastName) {
    return 1;
  }
  if (a.firstName < b.firstName) {
    return -1;
  }
  if (a.firstName > b.firstName) {
    return 1;
  }
  return 0;
};

const writeToCsv = (headers, data, outputFileName) => {
  const createCsvWriter = require("csv-writer").createObjectCsvWriter;
  const csvWriter = createCsvWriter({
    path: `${outputFileName}.csv`,
    header: headers.map((header) => {
      return { id: header, title: header };
    }),
  });

  csvWriter
    .writeRecords(data)
    .then(() => console.log(`${outputFileName}.csv written successfully`))
    .catch((e) => console.log(e));
};

const arrayData = [];

fs.createReadStream("./enrollments.csv")
  .on("error", (e) => console.log(e))
  .pipe(csv())
  .on("data", (row) => {
    arrayData.push(row);
  })
  .on("end", () => {
    const headers = Object.keys(arrayData[0]);
    const insuranceCompanies = groupByCarrier(arrayData);

    for (const property in insuranceCompanies) {
      insuranceCompanies[property] = Object.values(
        insuranceCompanies[property]
      );
      insuranceCompanies[property].sort(sortNames);

      writeToCsv(headers, insuranceCompanies[property], property);
    }
  });
