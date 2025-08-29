package pages
type PageType uint8

const (
	// BASIC TYPES
	NONE_PAGE PageType = iota
	FREE_PAGE
	VAR_PAGE
	MULTI_PAGE
	IDX_NODE
	IDX_LEAF
	META_TABLE_PAGE
	FSM

	// FILE TABLE & IDX
	FILE_FIXED
	FTYPE_IDX
	FTIME_IDX
	FID_IDX

	// LOGGER PAGE
	LOGGER_PAGE
)

func (p PageType) String() string {
	switch p {
	case NONE_PAGE: return "NONE_PAGE"
	case FREE_PAGE: return "FREE_PAGE"
	case VAR_PAGE: return "VAR_PAGE"
	case MULTI_PAGE: return "MULTI_PAGE"
	case IDX_NODE: return "IDX_NODE"
	case IDX_LEAF: return "IDX_LEAF"
	case META_TABLE_PAGE: return "META_TABLE_PAGE"
	case FSM: return "FSM"

	case FILE_FIXED: return "FILE_FIXED"
	case FTYPE_IDX: return "FTYPE_IDX"
	case FTIME_IDX: return "FTIME_IDX"
	case FID_IDX: return "FID_IDX"

	case LOGGER_PAGE: return "LOGGER_PAGE"
	}
	return "UNKNOWN_PAGE_TYPE"
}
