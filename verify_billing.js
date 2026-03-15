
// Mocking the behavior of billBooking logic
function billBookingMock(booking) {
    const logger = { error: (msg) => console.log('LOGGER ERROR:', msg) };

    if (!booking.isPlatformManagedUserBooking) {
        console.log('Skipping: Not platform managed');
        return;
    }

    const hostId = booking.userId || booking.hosts?.[0]?.id;
    if (!hostId || isNaN(Number(hostId))) {
        logger.error(`Booking with uid=${booking.uid} has no valid host ID (hostId=${hostId})`);
        return;
    }

    console.log(`SUCCESS: Billing for hostId=${hostId} numeric=${Number(hostId)}`);
}

console.log('--- Test 1: Valid hostId in hosts array ---');
billBookingMock({
    uid: 'bk1',
    hosts: [{ id: 123 }],
    isPlatformManagedUserBooking: true
});

console.log('--- Test 2: Valid userId directly ---');
billBookingMock({
    uid: 'bk2',
    userId: 456,
    hosts: [{ id: 123 }], // should prefer userId
    isPlatformManagedUserBooking: true
});

console.log('--- Test 3: Missing hostId and userId ---');
billBookingMock({
    uid: 'bk3',
    hosts: [],
    isPlatformManagedUserBooking: true
});

console.log('--- Test 4: NaN hostId (string) ---');
billBookingMock({
    uid: 'bk4',
    userId: 'abc', // non-numeric string
    isPlatformManagedUserBooking: true
});

console.log('--- Test 5: falsy hostId (0) ---');
billBookingMock({
    uid: 'bk5',
    userId: 0,
    isPlatformManagedUserBooking: true
});

console.log('--- Test 6: null hostId ---');
billBookingMock({
    uid: 'bk6',
    userId: null,
    isPlatformManagedUserBooking: true
});

console.log('--- Test 7: Not platform managed ---');
billBookingMock({
    uid: 'bk7',
    userId: 789,
    isPlatformManagedUserBooking: false
});
