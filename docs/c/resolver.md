# Resolver

The rules are in [README.md](README.md); signatures are those of `weida.h`. Every function here
is callable from any thread (R5).

A resolver decides what a `weida://` authority means (`weida::Resolver`): the system resolver
answers A and AAAA records, and a deployment that knows better (DNS SRV, a service registry, a
table) brings its own. The ABI takes no callbacks (decision 10 of
[decisions/0045](../decisions/0045-the-c-abi-is-the-boundary.md)), so a C resolver is a queue:
every `Resolver::resolve` call becomes a lookup the caller reads with `weida_resolver_recv` and
answers with `weida_resolved_answer` or `weida_resolved_refuse`. A configuration takes the
resolver with `weida_runtime_config_set_resolver` ([runtime.md](runtime.md)).

## `weida_resolver`

A try handle (R3a): `recv` takes the next lookup on the caller's thread.

| Function | Blocks | Returns |
| --- | --- | --- |
| `int weida_resolver_new(weida_resolver **out)` | never | 0; `-WEIDA_EINVAL` for a NULL `out` |
| `int weida_resolver_fd(const weida_resolver *r, int64_t *out)` | never | 0; `-WEIDA_EIO` when the descriptor pair cannot be made; `-WEIDA_EINVAL` |
| `int weida_resolver_recv(const weida_resolver *r, int64_t timeout_ms, weida_resolved **out)` | up to `timeout_ms` (R4) | 0 and the next lookup; 0 and NULL once no configuration and no runtime holds the resolver (one never set on a configuration never ends); `-WEIDA_EAGAIN`, `-WEIDA_EINTR`, `-WEIDA_ECLOSED` (dropped); `-WEIDA_EINVAL` |
| `void weida_resolver_drop(weida_resolver *r)` | never | — A thread blocked in `recv` returns `-WEIDA_ECLOSED`; the lookups still queued and every later one fail their dial with `-WEIDA_EINVALID_ADDRESS` |
| `int weida_runtime_config_set_resolver(const weida_runtime_config *cfg, const weida_resolver *r)` | never | 0; `-WEIDA_EINVAL`; `-WEIDA_ECLOSED` when `r`'s queue already ended. Shares `r`'s queue; NULL is the system resolver (`SystemResolver`) |

## `weida_resolved`

One lookup waiting for its answer (`Resolved`).

| Function | Blocks | Returns |
| --- | --- | --- |
| `const char *weida_resolved_name(const weida_resolved *q, size_t *len_out)` | never | the host the URL wrote (R7), valid until `q` is consumed or dropped; NULL for NULL |
| `int weida_resolved_port(const weida_resolved *q, uint16_t *out)` | never | 1 and the port when the URL wrote one; 0 when it wrote none, also for a NULL `q` or `out` |
| `uint64_t weida_resolved_max_addresses(const weida_resolved *q)` | never | the most addresses the answer may hold (`max_resolved_addresses`); 0 for NULL |
| `int weida_resolved_answer(weida_resolved *q, const char *const *addrs, size_t n)` | never; **consumes** `q` | 0; `-WEIDA_EINVAL` for no address, more than `max_addresses`, a NULL array or element, a non-UTF-8 one, or a text that is not an IP address and port, and the dial then fails with `-WEIDA_EINVALID_ADDRESS` naming why; `-WEIDA_ECLOSED` (answered already) |
| `int weida_resolved_refuse(weida_resolved *q, const char *reason)` | never; **consumes** `q` | 0; `-WEIDA_EINVAL` for a NULL or non-UTF-8 `reason`, which still refuses; `-WEIDA_ECLOSED` (answered already) |
| `void weida_resolved_drop(weida_resolved *q)` | never | — An unanswered lookup fails its dial with `-WEIDA_EINVALID_ADDRESS` ("the lookup was dropped unanswered") |

## The rules

- **Every dial and redial asks**, IP literals included: a runtime made with the resolver never
  resolves on its own.
- **A port absent means a set.** `weida_resolved_port` returns 0 when the URL wrote no port: the
  authority names a set ([decisions/0020](../decisions/0020-cluster-and-discovery.md) §4.2) and
  the answer chooses each address's port.
- **The answer is 1 to `max_addresses` `ip:port` texts in dial order** (`127.0.0.1:4433`,
  `[::1]:4433`): the dial tries them in that order. Anything else is `-WEIDA_EINVAL` to the
  caller and fails the dial; nothing is truncated.
- **A refusal fails the dial.** Refusing, dropping a lookup and dropping the resolver fail the
  dial with `-WEIDA_EINVALID_ADDRESS`, message `cannot resolve <name>: <reason>`. The first dial
  reports it to the connect op.
- **A dial waits as long as its own deadline allows** (`send_timeout`, the connect attempt): a
  lookup answered after its dial gave up is discarded, and the answer still returns 0.
- **The queue holds 64 unread lookups**; a dial beyond them waits for a place.
- **The queue ends with its holders.** The handle keeps the queue only until it is first set on
  a configuration; from then on the configurations and the runtimes made from them hold it, and
  `recv` returns NULL once the last of them is gone.
- Lookups may be answered on any thread, in any order.
