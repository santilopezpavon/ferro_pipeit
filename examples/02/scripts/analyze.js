const fs = require('fs');

console.log("TASK:", process.env.PIPEIT_TASK_NAME_ID);

const input = process.env.PIPEIT_IN_PROCESSED;
const output = process.env.PIPEIT_OUT_REPORT;

const data = fs.readFileSync(input, 'utf8');
const lines = data.split('\n').filter(Boolean);

const result = `Total lines: ${lines.length}\n`;

fs.writeFileSync(output, result);