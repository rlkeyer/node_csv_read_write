const csv = require("csv-parser");
const fs = require("fs");

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

const groupByCarrier = (data) => {
  let carriers = {};
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

let arrayData = [];

fs.createReadStream("./enrollments.csv")
  .pipe(csv())
  .on("data", (row) => {
    arrayData.push(row);
  })
  .on("end", () => {
    const headers = Object.keys(arrayData[0]);
    let insuranceCompanies = groupByCarrier(arrayData);

    for (const property in insuranceCompanies) {
      insuranceCompanies[property] = Object.values(
        insuranceCompanies[property]
      );
      insuranceCompanies[property].sort(sortNames);

      const createCsvWriter = require("csv-writer").createObjectCsvWriter;
      const csvWriter = createCsvWriter({
        path: `${property}.csv`,
        header: headers.map((header) => {
          return { id: header, title: header };
        }),
      });

      csvWriter
        .writeRecords(insuranceCompanies[property])
        .then(() => console.log(`${property}.csv written successfully`));
    }
  });
