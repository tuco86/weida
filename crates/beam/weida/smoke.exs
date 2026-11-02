# What the package must do from Elixir: load the NIF and its libweida, and
# make one round trip over the transport named by the first argument,
# `inproc` or `quic` (a generated identity, pinned).
#
# Usage: elixir -pa _build/default/lib/weida/ebin smoke.exs <inproc|quic>
#        (after `rebar3 compile` with WEIDA_LIB_DIR set)
transport =
  case System.argv() do
    [t] when t in ["inproc", "quic"] -> t
    _ -> raise "usage: smoke.exs <inproc|quic>"
  end

{:ok, runtime} = :weida.runtime_owned(%{})
{:ok, listener} = :weida.runtime_listener(runtime)
{:ok, trust} = :weida.trust_new()

{binding, url} =
  case transport do
    "quic" ->
      {:ok, identity} = :weida.identity_generate()
      {:ok, fingerprint} = :weida.identity_fingerprint(identity)
      {:ok, server_tls} = :weida.server_tls_new(identity)
      {:ok, binding} = :weida.listener_bind_quic(listener, "127.0.0.1:0", server_tls, :infinity)
      :ok = :weida.trust_add_pin(trust, fingerprint)
      {binding, "weida://#{fingerprint}@#{:weida.binding_local_addr(binding)}/echo"}

    "inproc" ->
      bus = "smoke-#{System.unique_integer([:positive])}"
      {:ok, binding} = :weida.listener_bind_inproc(listener, bus)
      {binding, "weida+inproc://#{bus}/echo"}
  end

{:ok, replier} = :weida.listener_replier(listener, "/echo")
{:ok, client_tls} = :weida.client_tls_new(trust)
{:ok, requester} = :weida.runtime_requester(runtime, client_tls)
:ok = :weida.requester_connect(requester, url, :infinity)

served =
  Task.async(fn ->
    {:ok, request} = :weida.replier_accept(replier, :infinity)
    {:ok, body} = :weida.incoming_request_take_body(request)
    {:ok, %{payload: payload}} = :weida.incoming_transfer_collect(body, 1024, :infinity)
    {:ok, reply} = :weida.incoming_request_reply(request, %{content_type: "text/plain"}, :infinity)
    :ok = :weida.outgoing_transfer_write_all(reply, String.reverse(payload), :infinity)
    {:ok, delivery} = :weida.outgoing_transfer_finish(reply)
    :ok = :weida.delivery_delivered(delivery, :infinity)
  end)

{:ok, reply} = :weida.requester_request(requester, "ping", :infinity)
{:ok, %{payload: payload, meta: meta}} = :weida.incoming_transfer_collect(reply, 1024, :infinity)
:ok = Task.await(served, :infinity)

if payload != "gnip" or meta.content_type != "text/plain" do
  raise "unexpected reply #{inspect(payload)} (#{inspect(meta.content_type)})"
end

for handle <- [requester, replier, binding, listener], do: :weida.drop(handle)
:ok = :weida.runtime_shutdown(runtime, :infinity)
IO.puts("beam smoke: ok (elixir #{transport}) libweida #{:weida.version()}")
