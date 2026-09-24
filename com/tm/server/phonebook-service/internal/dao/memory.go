package dao

import (
	"context"
	"slices"
	"sync"
)

// MemoryPhonebookDao: PhonebookDao trong bộ nhớ, cùng ngữ nghĩa CheckAndPut với HBase — dùng cho test.
type MemoryPhonebookDao struct {
	mu      sync.Mutex
	users   map[string]UserRow
	devices map[string]map[string]DeviceRow
	// Hook chạy trước mỗi CAS (test chèn ghi đồng thời)
	BeforeCAS func()
}

func NewMemoryPhonebookDao() *MemoryPhonebookDao {
	return &MemoryPhonebookDao{users: map[string]UserRow{}, devices: map[string]map[string]DeviceRow{}}
}

func (m *MemoryPhonebookDao) LoadUser(_ context.Context, userID string) (UserRow, []DeviceRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u := cloneUser(m.users[userID])
	var devs []DeviceRow
	for _, d := range m.devices[userID] {
		devs = append(devs, cloneDevice(d))
	}
	slices.SortFunc(devs, func(a, b DeviceRow) int {
		switch {
		case a.DeviceID < b.DeviceID:
			return -1
		case a.DeviceID > b.DeviceID:
			return 1
		}
		return 0
	})
	return u, devs, nil
}

func (m *MemoryPhonebookDao) LoadCheck(_ context.Context, userID, deviceID string) (UserMeta, *DeviceRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u := m.users[userID]
	meta := UserMeta{Ver: u.Ver, Lease: u.Lease, PendingSync: u.PendingSync, Count: u.Count, LastEvent: u.LastEvent}
	d, ok := m.devices[userID][deviceID]
	if !ok {
		return meta, nil, nil
	}
	light := DeviceRow{DeviceID: d.DeviceID, RootMAC: slices.Clone(d.RootMAC), Count: d.Count,
		SyncedAt: d.SyncedAt, DigestV: d.DigestV, Digests: slices.Clone(d.Digests)}
	return meta, &light, nil
}

func (m *MemoryPhonebookDao) condOK(userID string, c Cond) bool {
	u := m.users[userID]
	switch c.Column {
	case ColVer:
		return u.Ver == c.Value
	case ColPendingSync:
		return u.Ver != 0 && u.PendingSync == c.Value
	}
	return false
}

func (m *MemoryPhonebookDao) CASUserMeta(_ context.Context, userID string, c Cond, meta UserMeta) (bool, error) {
	if m.BeforeCAS != nil {
		m.BeforeCAS()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.condOK(userID, c) {
		return false, nil
	}
	u := m.users[userID]
	u.Ver, u.Lease, u.PendingSync, u.Count, u.LastEvent = meta.Ver, meta.Lease, meta.PendingSync, meta.Count, meta.LastEvent
	m.users[userID] = u
	return true, nil
}

func (m *MemoryPhonebookDao) CASUser(_ context.Context, userID string, c Cond, u UserRow) (bool, error) {
	if m.BeforeCAS != nil {
		m.BeforeCAS()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.condOK(userID, c) {
		return false, nil
	}
	m.users[userID] = cloneUser(u)
	return true, nil
}

func (m *MemoryPhonebookDao) PutDevice(_ context.Context, userID string, d DeviceRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.devices[userID] == nil {
		m.devices[userID] = map[string]DeviceRow{}
	}
	m.devices[userID][d.DeviceID] = cloneDevice(d)
	return nil
}

func (m *MemoryPhonebookDao) DeleteDevice(_ context.Context, userID, deviceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.devices[userID], deviceID)
	return nil
}

func cloneUser(u UserRow) UserRow {
	u.Published = slices.Clone(u.Published)
	u.Pending = slices.Clone(u.Pending)
	return u
}

func cloneDevice(d DeviceRow) DeviceRow {
	d.RootMAC = slices.Clone(d.RootMAC)
	d.Phones = slices.Clone(d.Phones)
	d.Names = slices.Clone(d.Names)
	d.Digests = slices.Clone(d.Digests)
	return d
}
