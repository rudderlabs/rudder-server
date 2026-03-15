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

// Step 1: Commit Issue 5 changes on fix/slug-conflict-28190
run('git add packages/trpc/server/routers/viewer/eventTypes/heavy/update.handler.ts packages/trpc/server/routers/viewer/eventTypes/heavy/create.handler.ts');
run('git commit -m "fix: resolve slug conflict between Managed and Round Robin event types (#28190)\n\n- Add random suffix to DB slug when changing schedulingType to MANAGED\n- Revert to clean slug when changing away from MANAGED type\n- Store real user-facing slug in metadata.managedEventProfileSlug\n- Improve error messages to distinguish team vs user slug conflicts\n- Catch Prisma P2002 errors and throw 400 instead of 500"');

// Step 2: Push Issue 5 branch
run('git push fork fix/slug-conflict-28190');

// Step 3: Switch back to main and create Issue 6 branch
run('git checkout -f origin/main');
run('git branch -D fix/booking-api-500-28245 2>nul || echo "branch did not exist"');
run('git checkout -b fix/booking-api-500-28245');

// Step 4: Extract Issue 6 files
run('git checkout fix/multiple-issues-platform -- apps/api/v2/src/ee/bookings/2024-08-13/services/bookings.service.ts');
run('git status');

// Step 5: Commit Issue 6
run('git add apps/api/v2/src/ee/bookings/2024-08-13/services/bookings.service.ts');
run('git commit -m "fix: resolve 500 error in booking creation API v2 (#28245)\n\n- Add strict numeric validation for userId in billBooking and billRescheduledBooking\n- Implement fallback from userId to hosts[0].id with NaN checks\n- Log error and skip billing gracefully instead of crashing with 500\n- Return userId consistently in all booking type outputs (regular, instant, recurring, seated, rescheduled)\n- Update CreatedBooking type to include userId"');

// Step 6: Push Issue 6 branch
run('git push fork fix/booking-api-500-28245');

// Step 7: Switch back to original branch
run('git checkout fix/multiple-issues-platform');
run('git log -n 3 --oneline');
