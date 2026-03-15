const dotenv = require("dotenv");
const fs = require("fs");
fs.writeFileSync(".env", "TEST_VAR=env");
fs.writeFileSync(".env.local", "TEST_VAR=env_local");

const result = {};
dotenv.config({
  processEnv: result,
  path: [".env", ".env.local"]
});
console.log(result);

fs.unlinkSync(".env");
fs.unlinkSync(".env.local");
