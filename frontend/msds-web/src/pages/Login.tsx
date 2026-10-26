import { useState, type FormEvent } from "react";
import { login } from "../api";

export function Login({ onDone }: { onDone: () => void }) {
  const [key, setKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await login(key);
      onDone();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="card login" onSubmit={submit}>
      <h1>MSDS 검토</h1>
      <label>
        API 키
        <input type="password" value={key} onChange={(e) => setKey(e.target.value)} autoFocus required />
      </label>
      <button disabled={busy || !key}>{busy ? "확인 중…" : "로그인"}</button>
      {error && <p className="error">{error}</p>}
    </form>
  );
}
