const fs = require('fs');

try {
    let raw = fs.readFileSync('refined_issues.json', 'utf8');
    raw = raw.replace(/^\uFEFF/, '');
    const issues = JSON.parse(raw);

    const easyLabels = ['good first issue', 'documentation', 'question', 'dependencies', 'chore', 'stale', 'need more info'];

    const hardIssues = issues.filter(issue => {
        const labels = issue.labels.map(l => l.name.toLowerCase());
        const isEasy = labels.some(l => easyLabels.includes(l));
        
        // Prioritize issues that have labels like bug, enhancement, high priority, etc.
        const isHardIndicator = labels.some(l => 
            l.includes('bug') || 
            l.includes('enhancement') || 
            l.includes('p1') || 
            l.includes('p2') || 
            l.includes('high') ||
            l.includes('critical') ||
            l.includes('investigate')
        );

        return !isEasy && (isHardIndicator || labels.length >= 2);
    });

    hardIssues.sort((a, b) => b.labels.length - a.labels.length);

    const top5 = hardIssues.slice(0, 5);

    let mdContent = "Here are 5 complex/hard issues from `rudder-server` to work on:\n\n";
    
    if (top5.length === 0) {
        mdContent += "Could not find any clearly 'hard' open issues that aren't stale or needing more info.\n";
    }

    top5.forEach((issue, index) => {
        mdContent += `### ${index + 1}. [${issue.title}](${issue.url})\n`;
        mdContent += `- **Issue #:** ${issue.number}\n`;
        mdContent += `- **Labels:** ${issue.labels.map(l => l.name).join(', ') || 'None'}\n\n`;
    });

    fs.writeFileSync('c:\\Users\\hp\\.gemini\\antigravity\\brain\\4b94182a-9fff-42d8-86b3-3fe03b688fee\\hard_issues.md', mdContent, 'utf8');
    console.log("Successfully wrote hard_issues.md artifact");

} catch (err) {
    console.error("Error processing issues:", err);
}
