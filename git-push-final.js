const { execSync } = require('child_process');
const opts = { cwd: 'c:\\Users\\hp\\Downloads\\G3\\cal.com', encoding: 'utf-8', stdio: 'pipe' };

function run(cmd) {
    try {
        console.log('--- Running:', cmd, '---');
        const result = execSync(cmd, opts).trim();
        if (result) console.log(result);
        return result;
    } catch (e) {
        console.error('Error:', (e.stdout || '').trim(), (e.stderr || '').trim());
        return null;
    }
}

// Save current fix for separator row nesting
run('git add apps/web/modules/data-table/components/DataTable.tsx');
run('git commit -m "WIP: save separator fix"');

// Create clean branch for Issue 2 (#28184)
run('git checkout -f origin/main');
run('git branch -D fix/separator-row-nesting-28184 2>nul || echo ""');
run('git checkout -b fix/separator-row-nesting-28184');
run('git checkout fix/multiple-issues-platform -- apps/web/modules/data-table/components/DataTable.tsx');
run('git commit -m "fix: resolve invalid DOM nesting in SeparatorRowRenderer (#28184)\n\n- Replace div with TableCell inside TableRow to ensure valid HTML structure"');
run('git push fork fix/separator-row-nesting-28184');

// Switch back to multiple-issues branch
run('git checkout fix/multiple-issues-platform');
console.log('--- All 5 PR branches pushed successfully! ---');
run('git branch -a');
