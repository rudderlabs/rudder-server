const fs = require('fs');
let raw = fs.readFileSync('issues.json', 'utf8');
raw = raw.replace(/^\uFEFF/, '');
const issues = JSON.parse(raw);

const easyLabels = ['good first issue', 'documentation', 'question', 'dependencies', 'chore'];

const hardIssues = issues.filter(issue => {
    const labels = issue.labels.map(l => l.name.toLowerCase());
    const isEasy = labels.some(l => easyLabels.includes(l));
    return !isEasy;
});

hardIssues.sort((a, b) => b.labels.length - a.labels.length);

const top5 = hardIssues.slice(0, 5);

let mdContent = "Here are 5 complex/hard issues from Rudder-Server to work on:\n\n";
top5.forEach((issue, index) => {
    mdContent += `### ${index + 1}. [${issue.title}](${issue.url})\n`;
    mdContent += `- **Issue #:** ${issue.number}\n`;
    mdContent += `- **Labels:** ${issue.labels.map(l => l.name).join(', ')}\n\n`;
});

fs.writeFileSync('c:\\Users\\hp\\.gemini\\antigravity\\brain\\4b94182a-9fff-42d8-86b3-3fe03b688fee\\hard_issues.md', mdContent, 'utf8');
