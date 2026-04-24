#include "parallel/pipeline/physical_operator.hpp"

#include "parallel/pipeline/meta_pipeline.hpp"
#include "parallel/pipeline/pipeline.hpp"

namespace pg_volvec {
namespace pipeline {

void
PhysicalOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta)
{
	if (IsSink())
	{
		meta.SetSink(current, *this);

		Pipeline &producer = meta.CreateChildPipeline(current, *this);
		Assert(children_.size() == 1);
		children_[0]->BuildPipelines(producer, meta);
		return;
	}

	if (children_.empty())
	{
		Assert(IsSource());
		meta.SetSource(current, *this);
		return;
	}

	Assert(children_.size() == 1);
	meta.AddOperator(current, *this);
	children_[0]->BuildPipelines(current, meta);
}

}  /* namespace pipeline */
}  /* namespace pg_volvec */
