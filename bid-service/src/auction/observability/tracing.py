"""Controle do tracing do OpenTelemetry para o que roda fora de uma requisição."""

import contextlib
from collections.abc import Iterator


@contextlib.contextmanager
def untraced() -> Iterator[None]:
    """Contexto sem tracing para as tarefas de fundo.

    O closer (a cada 200 ms), o reconciliador e o consumer gerariam traces raiz o tempo todo, sem
    requisição nenhuma: ruído no X-Ray e, mesmo a 5%, ~1,3 milhão de traces/mês com 2 tasks, acima do
    free tier. As tarefas nascem sob um span pai marcado como "não amostrado": o sampler
    parentbased_traceidratio segue o pai, então todo span filho (redis, asyncpg) é descartado. O
    suppress_instrumentation do OpenTelemetry não serviria: os instrumentadores de redis e asyncpg o
    ignoram. As tasks do asyncio copiam o contexto em que são criadas. Sem OpenTelemetry, não faz nada.
    """
    try:
        from opentelemetry import context, trace
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
    except ImportError:
        yield
        return
    unsampled = NonRecordingSpan(SpanContext(trace_id=1, span_id=1, is_remote=False,
                                             trace_flags=TraceFlags(TraceFlags.DEFAULT)))
    token = context.attach(trace.set_span_in_context(unsampled))
    try:
        yield
    finally:
        context.detach(token)
