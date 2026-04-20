const fs = require('fs');
const os = require('os');

const taskName = process.env.TASK_NAME_ID;
const reportPath = process.env.IN_REPORT;

console.log(`[${taskName}] Starting...`);

// Parse key=value report
const report = Object.fromEntries(
  fs.readFileSync(reportPath, 'utf8')
    .trim().split('\n')
    .map(l => l.split('='))
);

console.log(`
╔══════════════════════════════════════════════════════════╗
  PIPELINE COMPLETADO EN: ${os.hostname()}
╚══════════════════════════════════════════════════════════╝
  - Autor:          ${report.autor}
  - Items:          ${report.total_items}
  - Estado Final:   ${report.status}
────────────────────────────────────────────────────────────
`);

console.log(`[${taskName}] Done.`);