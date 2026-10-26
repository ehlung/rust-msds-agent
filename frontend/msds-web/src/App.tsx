import { useEffect, useState } from "react";
import { getToken, setToken } from "./api";
import { JobProgress } from "./pages/JobProgress";
import { Jobs } from "./pages/Jobs";
import { Login } from "./pages/Login";
import { Review } from "./pages/Review";

// 주소 해시로 화면을 고른다: #/ 목록, #/jobs/<id> 진행 상태, #/jobs/<id>/review 검토
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

  const m = /^#\/jobs\/([0-9a-f-]{36})(\/review)?$/.exec(hash);
  const jobId = m?.[1];
  const page = !jobId ? (
    <Jobs open={(id) => go(`#/jobs/${id}`)} />
  ) : m?.[2] ? (
    <Review jobId={jobId} back={() => go(`#/jobs/${jobId}`)} />
  ) : (
    <JobProgress jobId={jobId} back={() => go("#/")} review={() => go(`#/jobs/${jobId}/review`)} />
  );
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
      {page}
    </main>
  );
}
