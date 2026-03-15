const { execSync } = require('child_process');
const opts = { cwd: 'c:\\Users\\hp\\Downloads\\G3\\cal.com', encoding: 'utf-8', stdio: 'pipe' };

function run(cmd) {
    try {
        console.log(`--- Running: ${cmd} ---`);
        console.log(execSync(cmd, opts));
    } catch (e) {
        console.log(`Error running ${cmd}:`, e.stdout || e.message);
    }
}

run('git add .');
run('git commit -m "WIP: save current state"');
run('git branch -a');
run('git log -n 5 --oneline');
run('git remote -v');
