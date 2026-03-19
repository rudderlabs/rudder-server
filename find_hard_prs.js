const { execSync } = require('child_process');
const fs = require('fs');

// Fetch recent PRs from last 7 days
const cmd = `gh pr list --repo rudderlabs/rudder-server --state all --limit 100 --json number,title,author,createdAt,labels,url --search "created:>2025-03-12"`;
const output = execSync(cmd, { encoding: 'utf8', cwd: 'c:\\Users\\hp\\Downloads\\Rudder-Server' });
const prs = JSON.parse(output);

// Easy labels to filter out
const easyLabels = ['good first issue', 'documentation', 'question', 'dependencies', 'chore', 'autorelease: tagged', 'autorelease: pending'];

// Filter hard PRs (non-automated, non-trivial)
const hardPRs = prs.filter(pr => {
    const labels = pr.labels.map(l => l.name.toLowerCase());
    const isEasy = labels.some(l => easyLabels.includes(l));
    const isBot = pr.author.is_bot || pr.author.login.includes('dependabot') || pr.author.login.includes('github-actions');
    const isChore = pr.title.toLowerCase().startsWith('chore:');
    const isBump = pr.title.toLowerCase().includes('bump');
    const isRelease = pr.title.toLowerCase().includes('release') || pr.title.toLowerCase().includes('prerelease');
    return !isEasy && !isBot && !isChore && !isBump && !isRelease;
});

// Sort by number (recent first)
hardPRs.sort((a, b) => b.number - a.number);

// Top 5
const top5 = hardPRs.slice(0, 5);

console.log("\n=== TOP 5 HARD/COMPLEX PRs (Last 7 Days) ===\n");
top5.forEach((pr, index) => {
    console.log(`${index + 1}. PR #${pr.number}: ${pr.title}`);
    console.log(`   Author: ${pr.author.login}`);
    console.log(`   URL: ${pr.url}`);
    console.log(`   Labels: ${pr.labels.map(l => l.name).join(', ') || 'None'}`);
    console.log(`   Created: ${pr.createdAt.split('T')[0]}`);
    console.log();
});
