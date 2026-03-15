const superjson = require("superjson");

const data = {
    dateString: "2023-01-01T00:00:00.000Z",
    notADate: "Just a string",
    dateLike: "Tuesday, March 4th, 2025"
};

const stringified = superjson.stringify(data);
console.log("Stringified:", stringified);

const parsed = superjson.parse(stringified);
console.log("Parsed:", parsed);
console.log("dateString type:", typeof parsed.dateString, parsed.dateString instanceof Date ? "is Date" : "is not Date");
console.log("dateLike type:", typeof parsed.dateLike, parsed.dateLike instanceof Date ? "is Date" : "is not Date");
