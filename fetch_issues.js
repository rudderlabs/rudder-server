const fs = require('fs');

async function searchIssues(repo) {
    const url = `https://api.github.com/search/issues?q=repo:${repo}+is:issue+is:open+no:assignee+sort:updated-desc&per_page=30`;
    const res = await fetch(url, { headers: { 'User-Agent': 'Node-Fetch' } });
    if (!res.ok) {
        console.error(`Failed to fetch for ${repo}:`, res.statusText);
        return [];
    }
    const data = await res.json();
    return data.items || [];
}

async function main() {
    const repos = ['triggerdotdev/trigger.dev', 'calcom/cal.com', 'rudderlabs/rudder-server'];
    let allIssues = [];

    for (const repo of repos) {
        const issues = await searchIssues(repo);
        console.log(`Fetched ${issues.length} issues from ${repo}`);
        allIssues.push(...issues.map(i => ({
            repo,
            title: i.title,
            url: i.html_url,
            labels: i.labels.map(l => l.name),
            comments: i.comments,
            created_at: i.created_at,
            body: (i.body || '').substring(0, 200) + '...'
        })));
    }

    // Filter for potential medium issues by looking at labels or just taking ones without "docs" or too simple
    fs.writeFileSync('temp_issues.json', JSON.stringify(allIssues, null, 2));
    console.log('Saved to temp_issues.json');
}

main().catch(console.error);
