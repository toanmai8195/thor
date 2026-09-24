package dao

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

// HBasePhonebookDao: PhonebookDao trên gohbase.
type HBasePhonebookDao struct {
	client gohbase.Client
	table  string
}

// NewHBasePhonebookDao kết nối qua ZooKeeper quorum (vd "zk1:2181,zk2:2181"). Bảng phải tạo sẵn.
func NewHBasePhonebookDao(zkQuorum, table string) *HBasePhonebookDao {
	return &HBasePhonebookDao{client: gohbase.NewClient(zkQuorum), table: table}
}

func (h *HBasePhonebookDao) Close() { h.client.Close() }

// Ping: đọc 1 row không tồn tại để kiểm tra kết nối + bảng.
func (h *HBasePhonebookDao) Ping(ctx context.Context) error {
	get, err := hrpc.NewGetStr(ctx, h.table, "\x00ping", hrpc.Families(map[string][]string{FamMeta: {ColVer}}))
	if err != nil {
		return err
	}
	_, err = h.client.Get(get)
	return err
}

func (h *HBasePhonebookDao) LoadUser(ctx context.Context, userID string) (UserRow, []DeviceRow, error) {
	prefix := RowPrefix(userID)
	// '#' + 1 = '$': dừng ngay sau row cuối có prefix này
	stop := prefix[:len(prefix)-1] + "$"
	scan, err := hrpc.NewScanRangeStr(ctx, h.table, prefix, stop)
	if err != nil {
		return UserRow{}, nil, err
	}
	scanner := h.client.Scan(scan)
	defer scanner.Close()

	var user UserRow
	var devs []DeviceRow
	for {
		res, err := scanner.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return UserRow{}, nil, fmt.Errorf("scan %s: %w", prefix, err)
		}
		cells := toMap(res)
		if len(res.Cells) == 0 {
			continue
		}
		deviceID := strings.TrimPrefix(string(res.Cells[0].Row), prefix)
		if deviceID == "" {
			user = parseUser(cells)
		} else {
			devs = append(devs, parseDevice(deviceID, cells))
		}
	}
	return user, devs, nil
}

func (h *HBasePhonebookDao) LoadCheck(ctx context.Context, userID, deviceID string) (UserMeta, *DeviceRow, error) {
	ug, err := hrpc.NewGetStr(ctx, h.table, RowKey(userID, ""), hrpc.Families(map[string][]string{FamMeta: nil}))
	if err != nil {
		return UserMeta{}, nil, err
	}
	dg, err := hrpc.NewGetStr(ctx, h.table, RowKey(userID, deviceID),
		hrpc.Families(map[string][]string{FamMeta: nil, FamBlob: {ColDigests}}))
	if err != nil {
		return UserMeta{}, nil, err
	}
	ures, err := h.client.Get(ug)
	if err != nil {
		return UserMeta{}, nil, fmt.Errorf("get user: %w", err)
	}
	dres, err := h.client.Get(dg)
	if err != nil {
		return UserMeta{}, nil, fmt.Errorf("get device: %w", err)
	}
	u := parseUser(toMap(ures))
	meta := UserMeta{Ver: u.Ver, Lease: u.Lease, PendingSync: u.PendingSync, Count: u.Count, LastEvent: u.LastEvent}
	if len(dres.Cells) == 0 {
		return meta, nil, nil
	}
	d := parseDevice(deviceID, toMap(dres))
	return meta, &d, nil
}

func (h *HBasePhonebookDao) CASUserMeta(ctx context.Context, userID string, c Cond, m UserMeta) (bool, error) {
	return h.cas(ctx, userID, c, map[string]map[string][]byte{FamMeta: metaValues(m)})
}

func (h *HBasePhonebookDao) CASUser(ctx context.Context, userID string, c Cond, u UserRow) (bool, error) {
	vals := map[string]map[string][]byte{
		FamMeta: metaValues(UserMeta{Ver: u.Ver, Lease: u.Lease, PendingSync: u.PendingSync, Count: u.Count, LastEvent: u.LastEvent}),
		FamBlob: {ColPublished: nonNil(u.Published), ColPending: nonNil(u.Pending)},
	}
	return h.cas(ctx, userID, c, vals)
}

// cas: CheckAndPut trên cột c.Column của family m. Giá trị mong đợi rỗng = "cột chưa có"
// (HBase coi comparator rỗng là kiểm tra không tồn tại).
func (h *HBasePhonebookDao) cas(ctx context.Context, userID string, c Cond, vals map[string]map[string][]byte) (bool, error) {
	put, err := hrpc.NewPutStr(ctx, h.table, RowKey(userID, ""), vals)
	if err != nil {
		return false, err
	}
	var expected []byte
	if c.Value != 0 {
		expected = u64(c.Value)
	}
	ok, err := h.client.CheckAndPut(put, FamMeta, c.Column, expected)
	if err != nil {
		return false, fmt.Errorf("checkAndPut %s: %w", c.Column, err)
	}
	return ok, nil
}

func (h *HBasePhonebookDao) PutDevice(ctx context.Context, userID string, d DeviceRow) error {
	put, err := hrpc.NewPutStr(ctx, h.table, RowKey(userID, d.DeviceID), map[string]map[string][]byte{
		FamMeta: {
			ColRootMAC:  nonNil(d.RootMAC),
			ColCount:    u64(uint64(d.Count)),
			ColSyncedAt: u64(uint64(d.SyncedAt)),
			ColDigestV:  u64(uint64(d.DigestV)),
		},
		FamBlob: {ColPhones: nonNil(d.Phones), ColNames: nonNil(d.Names), ColDigests: nonNil(d.Digests)},
	})
	if err != nil {
		return err
	}
	_, err = h.client.Put(put)
	return err
}

func (h *HBasePhonebookDao) DeleteDevice(ctx context.Context, userID, deviceID string) error {
	del, err := hrpc.NewDelStr(ctx, h.table, RowKey(userID, deviceID), nil)
	if err != nil {
		return err
	}
	_, err = h.client.Delete(del)
	return err
}

func metaValues(m UserMeta) map[string][]byte {
	return map[string][]byte{
		ColVer:         u64(m.Ver),
		ColLease:       u64(uint64(m.Lease)),
		ColPendingSync: u64(m.PendingSync),
		ColCount:       u64(uint64(m.Count)),
		ColLastEvent:   u64(uint64(m.LastEvent)),
	}
}

// toMap: "family:qualifier" → value
func toMap(res *hrpc.Result) map[string][]byte {
	out := make(map[string][]byte, len(res.Cells))
	for _, c := range res.Cells {
		out[string(c.Family)+":"+string(c.Qualifier)] = c.Value
	}
	return out
}

func parseUser(c map[string][]byte) UserRow {
	return UserRow{
		Ver:         getU64(c, FamMeta, ColVer),
		Lease:       int64(getU64(c, FamMeta, ColLease)),
		PendingSync: getU64(c, FamMeta, ColPendingSync),
		Count:       int64(getU64(c, FamMeta, ColCount)),
		LastEvent:   int64(getU64(c, FamMeta, ColLastEvent)),
		Published:   c[FamBlob+":"+ColPublished],
		Pending:     c[FamBlob+":"+ColPending],
	}
}

func parseDevice(id string, c map[string][]byte) DeviceRow {
	return DeviceRow{
		DeviceID: id,
		RootMAC:  c[FamMeta+":"+ColRootMAC],
		Count:    int64(getU64(c, FamMeta, ColCount)),
		SyncedAt: int64(getU64(c, FamMeta, ColSyncedAt)),
		DigestV:  int64(getU64(c, FamMeta, ColDigestV)),
		Phones:   c[FamBlob+":"+ColPhones],
		Names:    c[FamBlob+":"+ColNames],
		Digests:  c[FamBlob+":"+ColDigests],
	}
}

func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func getU64(c map[string][]byte, fam, col string) uint64 {
	v := c[fam+":"+col]
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// nonNil: HBase nhận giá trị rỗng, nhưng map nil trong hrpc có thể bị bỏ qua → luôn gửi []byte{}.
func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// TableSplits: điểm chia region theo tiền tố hex của row key (tương đương HexStringSplit).
// n = số region, lấy n chia hết 65536 (vd 4, 16, 64).
func TableSplits(n int) [][]byte {
	var out [][]byte
	step := 0x10000 / n
	for i := 1; i < n; i++ {
		out = append(out, []byte(fmt.Sprintf("%04x", i*step)))
	}
	return out
}

// CreateTable tạo bảng nếu chưa có — chỉ dùng cho môi trường local / test;
// production do team HBase tạo theo lệnh trong PHONEBOOK.md mục 2.3.
func CreateTable(ctx context.Context, zkQuorum, table string, regions int) error {
	admin := gohbase.NewAdminClient(zkQuorum)
	families := map[string]map[string]string{
		FamMeta: {"VERSIONS": "1", "BLOOMFILTER": "ROW", "IN_MEMORY": "true"},
		FamBlob: {"VERSIONS": "1", "BLOOMFILTER": "ROW", "COMPRESSION": "NONE"},
	}
	err := admin.CreateTable(hrpc.NewCreateTable(ctx, []byte(table), families, hrpc.SplitKeys(TableSplits(regions))))
	if err != nil && strings.Contains(err.Error(), "TableExistsException") {
		return nil
	}
	return err
}
