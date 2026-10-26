import { useEffect, useState } from "react";
import { getToken, setToken } from "./api";
import { JobProgress } from "./pages/JobProgress";
import { Jobs } from "./pages/Jobs";
import { Login } from "./pages/Login";

// 주소 해시로 화면을 고른다: #/ 목록, #/jobs/<id> 진행 상태
function useHash(): [string, (h: string) => void] {
  const [hash, setHash] = useState(location.hash);
  useEffect(() => {
    const on = () => setHash(location.hash);
    addEventListener("hashchange", on);
    return () => removeEventListener("hashchange", on);
  }, []);
  return [hash, (h) => (location.hash = h)];
}

export function App() {
  const [loggedIn, setLoggedIn] = useState(getToken() !== null);
  const [hash, go] = useHash();

  if (!loggedIn) return <Login onDone={() => setLoggedIn(true)} />;

  const jobId = /^#\/jobs\/([0-9a-f-]{36})$/.exec(hash)?.[1];
  return (
    <main>
      <header>
        <a href="#/">MSDS 검토</a>
        <button
          className="link"
          onClick={() => {
            setToken(null);
            setLoggedIn(false);
          }}
        >
          로그아웃
        </button>
      </header>
      {jobId ? <JobProgress jobId={jobId} back={() => go("#/")} /> : <Jobs open={(id) => go(`#/jobs/${id}`)} />}
    </main>
  );
}
