/** An epoch fence for UI/network races, not a distributed fencing token. */
export type SessionKey = string;
export type OperationToken = Readonly<{ session: SessionKey; epoch: bigint }>;
export type Snapshot = Readonly<{ session: SessionKey; epoch: bigint; text: string; terminal: boolean }>;

class MutableSession {
  epoch = 0n;
  text = '';
  terminal = true;
}

export class EpochArbiter {
  private readonly sessions = new Map<SessionKey, MutableSession>();

  private state(session: SessionKey): MutableSession {
    let s = this.sessions.get(session);
    if (!s) { s = new MutableSession(); this.sessions.set(session, s); }
    return s;
  }

  /** Begin invalidates all previously issued tokens, even after cancel or error. */
  begin(session: SessionKey): OperationToken {
    const s = this.state(session);
    s.epoch += 1n;
    s.text = '';
    s.terminal = false;
    return Object.freeze({ session, epoch: s.epoch });
  }

  /** A late chunk cannot mutate the new generation or reopen a terminal operation. */
  append(token: OperationToken, text: string): boolean {
    const s = this.state(token.session);
    if (s.epoch !== token.epoch || s.terminal) return false;
    s.text += text;
    return true;
  }

  complete(token: OperationToken): boolean { return this.close(token); }
  cancel(token: OperationToken): boolean { return this.close(token); }
  fail(token: OperationToken): boolean { return this.close(token); }

  private close(token: OperationToken): boolean {
    const s = this.state(token.session);
    if (s.epoch !== token.epoch || s.terminal) return false;
    s.terminal = true;
    return true;
  }

  snapshot(session: SessionKey): Snapshot {
    const s = this.state(session);
    return Object.freeze({ session, epoch: s.epoch, text: s.text, terminal: s.terminal });
  }
}
