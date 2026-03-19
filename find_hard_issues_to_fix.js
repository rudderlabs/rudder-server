const { execSync } = require('child_process');

// Fetch open issues (more results)
const cmd = `gh issue list --repo rudderlabs/rudder-server --state open --limit 200 --json number,title,createdAt,labels,url,body`;
const output = execSync(cmd, { encoding: 'utf8', cwd: 'c:\\Users\\hp\\Downloads\\Rudder-Server' });
const issues = JSON.parse(output);

// Filter out invalid issues only
const skipLabels = ['invalid', 'duplicate', 'wontfix', 'good first issue'];

const hardIssues = issues.filter(issue => {
    const labels = issue.labels.map(l => l.name.toLowerCase());
    const isInvalid = labels.some(l => skipLabels.includes(l));
    
    // Complex keywords in title/body
    const text = (issue.title + ' ' + (issue.body || '')).toLowerCase();
    
    // Include bugs, errors, crashes, performance, connection issues, data issues
    const hardKeywords = ['error', 'bug', 'crash', 'panic', 'fail', 'timeout', 'connection', 'slow', 'performance', 'memory', 'leak', 'deadlock', 'race', 'goroutine', 'data loss', 'corruption', 'migration', 'staging', 'warehouse', 'destination'];
    const isHard = hardKeywords.some(kw => text.includes(kw));
    
    // Exclude simple questions
    const isSimpleQuestion = text.startsWith('how') || text.startsWith('what') || text.startsWith('where') || (text.includes('?') && text.includes('how to'));
    
    return !isInvalid && !isSimpleQuestion && isHard;
});

// Sort by complexity indicators
hardIssues.sort((a, b) => {
    const aText = (a.title + ' ' + a.body).toLowerCase();
    const bText = (b.title + ' ' + b.body).toLowerCase();
    const complexKeywords = ['memory leak', 'race condition', 'deadlock', 'performance', 'panic', 'crash'];
    const aScore = complexKeywords.filter(kw => aText.includes(kw)).length;
    const bScore = complexKeywords.filter(kw => bText.includes(kw)).length;
    return bScore - aScore;
});

// Top all
const allHard = hardIssues;

console.log(`\n=== ALL HARD ISSUES (${allHard.length} found) ===\n`);
allHard.forEach((issue, index) => {
    console.log(`${index + 1}. Issue #${issue.number}: ${issue.title}`);
    console.log(`   URL: ${issue.url}`);
    console.log(`   Labels: ${issue.labels.map(l => l.name).join(', ') || 'None'}`);
    console.log(`   Created: ${issue.createdAt.split('T')[0]}`);
    
    // Show brief description
    const desc = issue.body ? issue.body.substring(0, 200).replace(/\n/g, ' ') + '...' : 'No description';
    console.log(`   Summary: ${desc}`);
    console.log();
});

console.log("Instructions:");
console.log("1. Pick an issue from above");
console.log("2. Fork the repo: gh repo fork rudderlabs/rudder-server");
console.log("3. Create branch: git checkout -b fix/issue-#{number}");
console.log("4. Fix the issue and commit");
console.log("5. Push: git push origin fix/issue-#{number}");
console.log("6. Create PR: gh pr create --title \"fix: ...\" --body \"Fixes #{number}\"");
