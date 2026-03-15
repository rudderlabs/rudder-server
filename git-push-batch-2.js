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

// Issue 4 (#28193) - Dayjs UTC
run('git checkout -f origin/main');
run('git branch -D fix/dashboard-utc-28193 2>nul || echo ""');
run('git checkout -b fix/dashboard-utc-28193');
run('git checkout fix/multiple-issues-platform -- packages/dayjs/index.ts');
run('git commit -m "fix: resolve dashboard UTC time display issue (#28193)\n\n- Ensure dayjs utc plugin is loaded before timezone plugin to correctly render local time"');
run('git push fork fix/dashboard-utc-28193');

// Issue 1 (#19159) - Hide settings
run('git checkout -f origin/main');
run('git branch -D feat/hide-individual-settings-19159 2>nul || echo ""');
run('git checkout -b feat/hide-individual-settings-19159');
const issue1Files = [
    'packages/features/eventtypes/lib/types.ts',
    'apps/web/modules/event-types/components/tabs/setup/EventSetupTab.tsx',
    'packages/platform/atoms/event-types/wrappers/EventTypePlatformWrapper.tsx',
    'packages/trpc/server/routers/viewer/eventTypes/util.ts'
];
run(`git checkout fix/multiple-issues-platform -- ${issue1Files.join(' ')}`);
run('git commit -m "feat: ability to hide individual settings in Event-Type atom (#19159)\n\n- Add hiddenSettings support in EventTypePlatformWrapper\n- Update EventSetupTab to respect individual hidden settings\n- Update types and utilities to support granular setting visibility"');
run('git push fork feat/hide-individual-settings-19159');

// Final check
run('git branch -a');
