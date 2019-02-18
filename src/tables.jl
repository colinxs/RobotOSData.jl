using Tables
using Dates

struct Source{NT, C}
    columns::C
    topic::String
end

_getdatacolumn(x, field) = [getproperty(row.data, field) for row in x]

function _getcolumn(x, field)
    if field === :time
        return [m.time for m in x]
    else
        return _getdatacolumn(x, field)
    end
end

function Source(bag, topic::String)
    topicarr = bag[topic]
    m1 = topicarr[1]

    colnames = (:time, propertynames(m1.data)...)
    columns = Tuple(_getcolumn(topicarr, f) for f in colnames)
    coltypes = Tuple(typeof(c[1]) for c in columns)


    #names = Vector{Symbol}(undef, ncols)
    #types = Vector{Symbol}(undef, ncols)
    #for (col, id) in enumerate(keys(bag.type_map))
    #    names[col] = Symbol(bag.topic_map[id])
    #    types[col] = bag.type_map[id]
    #end

    schema = NamedTuple{Tuple(colnames), Tuple{coltypes...}}

    Source{schema, typeof(columns)}(columns, topic)
end

Tables.istable(::Type{<:Source})= true
Tables.columnaccess(::Type{<:Source})= true
Tables.columns(s::Source{NamedTuple{names, T}}) where {names, T} = NamedTuple{names}(Tuple(s.columns[i] for i in 1:length(names)))
Tables.schema(s::Source{NT}) where NT = Tables.Schema(NT)