import { NavLink, Navigate, Route, Routes } from 'react-router-dom';
import { SessionBar } from './components/SessionBar';
import { FriendNetworkPage } from './pages/FriendNetworkPage';
import { LocalContactsPage } from './pages/LocalContactsPage';
import { ServerContactsPage } from './pages/ServerContactsPage';
import { SyncPage } from './pages/SyncPage';

export function App() {
  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">TM App</div>
        <nav className="nav">
          <NavLink to="/contacts">Danh bạ local</NavLink>
          <NavLink to="/sync">Đồng bộ</NavLink>
          <NavLink to="/server">Danh bạ server</NavLink>
          <NavLink to="/friends">Friend network</NavLink>
          <div className="hint">
            Danh bạ local lưu trong IndexedDB (Dexie) theo từng <b>user + thiết bị</b> ở thanh trên — đổi để giả lập nhiều
            máy / nhiều user.
          </div>
        </nav>
      </aside>
      <main className="main">
        <Routes>
          <Route path="/friends" element={<FriendNetworkPage />} />
          <Route
            path="*"
            element={
              <>
                <SessionBar />
                <Routes>
                  <Route path="/contacts" element={<LocalContactsPage />} />
                  <Route path="/sync" element={<SyncPage />} />
                  <Route path="/server" element={<ServerContactsPage />} />
                  <Route path="*" element={<Navigate to="/contacts" replace />} />
                </Routes>
              </>
            }
          />
        </Routes>
      </main>
    </div>
  );
}
