const fs = require('fs');

const issues = JSON.parse(fs.readFileSync('temp_issues.json', 'utf8'));

// Filter criteria: not docs, not good first issue (we want medium), has some labels
const isMedium = (issue) => {
    const lbls = issue.labels.map(l => l.toLowerCase());

    if (lbls.some(l => l.includes('docs') || l.includes('documentation'))) return false;

    // Exclude extremely simple ones if we want medium
    // If it's pure "good first issue" alone, maybe it's too simple. But some repo use it for everything.
    // We want to prioritize 'enhancement', 'bug', 'medium', 'help wanted'
    const isEnhancement = lbls.some(l => l.includes('enhancement') || l.includes('feature'));
    const isBug = lbls.some(l => l.includes('bug'));
    const isHelpWanted = lbls.some(l => l.includes('help wanted'));

    return isEnhancement || isBug || isHelpWanted || lbls.length > 0;
};

const candidateIssues = issues.filter(isMedium);

// Sort to mix repos and prioritize those with comments (some discussion means it's real) or certain labels
candidateIssues.sort((a, b) => b.comments - a.comments);

const top10 = candidateIssues.slice(0, 10);

let md = '# Medium Level PR Opportunities\n\n';
md += 'Here are 10 medium-level issues from repositories you have worked on (Trigger.dev, Cal.com) that you could potentially complete within 1 week.\n\n';

top10.forEach((issue, index) => {
    md += `## ${index + 1}. [${issue.title}](${issue.url}) (${issue.repo})\n`;
    md += `- **Labels:** ${issue.labels.join(', ') || 'None'}\n`;
    md += `- **Comments:** ${issue.comments}\n`;
    md += `- **Created:** ${new Date(issue.created_at).toLocaleDateString()}\n`;
    md += `- **Context:** ${issue.body.replace(/\r?\n/g, ' ')}\n\n`;
});

fs.writeFileSync('C:\\Users\\hp\\.gemini\\antigravity\\brain\\a8d88ee8-ea2f-4572-90c7-e43ece84ed85\\pr_recommendations.md', md);
console.log('Markdown generated at pr_recommendations.md');
