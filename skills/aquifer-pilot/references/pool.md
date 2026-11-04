# A pool of backends

Part of the aquifer-pilot skill, step 3. Read it when the backend is several interchangeable instances.

If the backend is several interchangeable instances (a fleet of GPU servers, a set of workers), don't put a load balancer in front of them and point Aquifer at that. Register them as a **pool** instead:
- Each instance registers with `POST /pools/{pool_id}/members` and keeps re-registering as a heartbeat. It declares its `capacity_rps`.
- Jobs then target `"pool_id"` instead of `"url"`.
- Aquifer picks a member for each dispatch, weighted by declared capacity × live reputation. A member declaring 100 rps gets about 4× the work of one declaring 25.
- A failure halves that member's share, and a success nudges it back up. A `5xx` is retried on another member.
- The pool's ceiling is the live sum of its members, so it grows and shrinks as instances register, degrade or drop out.

This is the most precise option Aquifer has: it paces each instance by what that instance says it can do, instead of spreading load evenly and hoping. For a pilot, record each member's declared capacity and its share of the work. Overload signals matter even more here: answer `429` or a lower `X-Aqueduct-Rps` when busy, because a `5xx` lowers that member's reputation.
