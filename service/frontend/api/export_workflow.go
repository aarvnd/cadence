package api

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/frontend/validate"
)

const exportVersion = 1

type exportContinuationToken struct {
	BranchToken []byte                   `json:"branchToken"`
	MaxEventID  int64                    `json:"maxEventID"`
	ShardID     int                      `json:"shardID"`
	Execution   *types.WorkflowExecution `json:"execution"`
	DomainID    string                   `json:"domainID"`
	DomainName  string                   `json:"domainName"`
	// ClusterName is the cluster that serves the export. The branch token and the persistence
	// token are local to it.
	ClusterName string `json:"clusterName"`
	// PersistenceToken is the internal pagination token from ReadRawHistoryBranch
	PersistenceToken []byte `json:"persistenceToken"`
	// CheckLastEvent is true until the page that has the requested last event checks its type.
	CheckLastEvent bool `json:"checkLastEvent"`
}

var errExportTokenFromOtherCluster = &types.BadRequestError{
	Message: "NextPageToken is from a different cluster. Start the export again without a NextPageToken.",
}

func (wh *WorkflowHandler) ExportWorkflowExecution(
	ctx context.Context,
	request *types.ExportWorkflowExecutionRequest,
) (*types.ExportWorkflowExecutionResponse, error) {
	if wh.isShuttingDown() {
		return nil, validate.ErrShuttingDown
	}
	if request == nil {
		return nil, validate.ErrRequestNotSet
	}

	domainName := request.GetDomain()
	if domainName == "" {
		return nil, validate.ErrDomainNotSet
	}
	if err := validate.CheckExecution(request.GetExecution()); err != nil {
		return nil, err
	}

	domainID, err := wh.GetDomainCache().GetDomainID(domainName)
	if err != nil {
		return nil, err
	}

	pageSize := int(request.GetPageSize())
	if pageSize <= 0 || pageSize > constants.GetHistoryMaxPageSize {
		pageSize = constants.GetHistoryMaxPageSize
	}

	var token *exportContinuationToken
	if len(request.GetNextPageToken()) == 0 {
		// First page: there is no token yet, so make a new one.
		token, err = wh.newExportContinuationToken(ctx, request, domainID)
		if err != nil {
			return nil, err
		}
	} else {
		// Next page: check that the token belongs to this request.
		token = &exportContinuationToken{}
		if err := json.Unmarshal(request.GetNextPageToken(), token); err != nil {
			return nil, &types.BadRequestError{Message: "invalid NextPageToken"}
		}
		if token.DomainID != domainID {
			return nil, &types.BadRequestError{Message: "NextPageToken domain does not match request domain"}
		}
		if token.ClusterName != wh.GetClusterMetadata().GetCurrentClusterName() {
			return nil, errExportTokenFromOtherCluster
		}
		if token.Execution.GetWorkflowID() != request.GetExecution().GetWorkflowID() {
			return nil, &types.BadRequestError{Message: "NextPageToken workflow does not match request workflow"}
		}
		if request.GetExecution().GetRunID() != "" && request.GetExecution().GetRunID() != token.Execution.GetRunID() {
			return nil, validate.ErrNextPageTokenRunIDMismatch
		}
	}

	rawResp, err := wh.GetHistoryManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   token.BranchToken,
		MinEventID:    constants.FirstEventID,
		MaxEventID:    token.MaxEventID,
		PageSize:      pageSize,
		NextPageToken: token.PersistenceToken,
		ShardID:       common.IntPtr(token.ShardID),
		DomainName:    token.DomainName,
	})
	if err != nil {
		return nil, err
	}
	lastPage := len(rawResp.NextPageToken) == 0

	// The requested last event is checked on the page that has the event.
	if token.CheckLastEvent {
		found, err := checkExportLastBatch(rawResp.HistoryEventBlobs, token.MaxEventID-1, lastPage)
		if err != nil {
			return nil, err
		}
		token.CheckLastEvent = !found
	}

	blobs := make([]*types.DataBlob, 0, len(rawResp.HistoryEventBlobs))
	for _, blob := range rawResp.HistoryEventBlobs {
		b := blob.ToInternal()
		b.Checksum = make([]byte, 4)
		binary.LittleEndian.PutUint32(b.Checksum, crc32.ChecksumIEEE(b.Data))
		blobs = append(blobs, b)
	}

	var nextPageToken []byte
	if !lastPage {
		token.PersistenceToken = rawResp.NextPageToken
		nextPageToken, err = json.Marshal(token)
		if err != nil {
			return nil, err
		}
	}

	return &types.ExportWorkflowExecutionResponse{
		Execution:      token.Execution,
		HistoryBatches: blobs,
		NextPageToken:  nextPageToken,
		Version:        exportVersion,
	}, nil
}

// newExportContinuationToken makes the token of the first page. It reads the current branch of the
// run and checks that the requested last event is in range.
func (wh *WorkflowHandler) newExportContinuationToken(
	ctx context.Context,
	request *types.ExportWorkflowExecutionRequest,
	domainID string,
) (*exportContinuationToken, error) {

	resp, err := wh.GetHistoryClient().GetMutableState(ctx, &types.GetMutableStateRequest{
		DomainUUID: domainID,
		Execution:  request.GetExecution(),
	})
	if err != nil {
		return nil, err
	}

	// A zombie lost a run conflict, so its history is not the history of the workflow.
	if resp.WorkflowState != nil && int(*resp.WorkflowState) == persistence.WorkflowStateZombie {
		return nil, &types.BadRequestError{Message: "Cannot export a zombie workflow run."}
	}

	versionHistories := persistence.NewVersionHistoriesFromInternalType(resp.GetVersionHistories())
	currentVersionHistory, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	lastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return nil, err
	}

	execution := resp.Execution
	if execution == nil {
		execution = request.GetExecution()
	}
	shardID := common.WorkflowIDToHistoryShard(execution.GetWorkflowID(), wh.config.NumHistoryShards)

	maxEventID := lastItem.EventID + 1
	if request.LastEventID != nil {
		lastEventID := *request.LastEventID
		if lastEventID <= 0 || lastEventID > lastItem.EventID {
			return nil, &types.BadRequestError{Message: "LastEventID is out of range"}
		}
		maxEventID = lastEventID + 1
	}

	return &exportContinuationToken{
		BranchToken:    currentVersionHistory.GetBranchToken(),
		MaxEventID:     maxEventID,
		ShardID:        shardID,
		Execution:      execution,
		DomainID:       domainID,
		DomainName:     request.GetDomain(),
		ClusterName:    wh.GetClusterMetadata().GetCurrentClusterName(),
		CheckLastEvent: request.LastEventID != nil,
	}, nil
}

// checkExportLastBatch checks the last batch of a page while the requested last event is not found.
// It returns true when the batch has the event, and an error when the last page does not have it.
// The export does not cut a batch, so the event must be the last event of its batch. Reset has the
// same rule for the event that it rebuilds the state to: the event before the decision finish event.
func checkExportLastBatch(blobs []*persistence.DataBlob, lastEventID int64, lastPage bool) (bool, error) {
	if len(blobs) > 0 {
		events, err := persistence.NewPayloadSerializer().DeserializeBatchEvents(blobs[len(blobs)-1])
		if err != nil {
			return false, err
		}
		if len(events) > 0 && events[len(events)-1].ID >= lastEventID {
			if batchLastEventID := events[len(events)-1].ID; batchLastEventID != lastEventID {
				return false, &types.BadRequestError{Message: fmt.Sprintf(
					"LastEventID %d must be the last event of a history batch, its batch ends at event %d", lastEventID, batchLastEventID,
				)}
			}
			return true, checkExportLastEvent(events[len(events)-1])
		}
	}
	if lastPage {
		return false, &types.BadRequestError{Message: fmt.Sprintf("LastEventID %d not found in the history", lastEventID)}
	}
	return false, nil
}

// checkExportLastEvent checks that the last event of a cut history ends a decision task, so the
// imported workflow can schedule its next decision.
func checkExportLastEvent(event *types.HistoryEvent) error {
	switch event.GetEventType() {
	case types.EventTypeDecisionTaskStarted,
		types.EventTypeDecisionTaskTimedOut,
		types.EventTypeDecisionTaskFailed:
		return nil
	case types.EventTypeDecisionTaskCompleted:
		return &types.BadRequestError{Message: fmt.Sprintf(
			"LastEventID cannot be a DecisionTaskCompleted event: use the DecisionTaskStarted event ID %d", event.ID-1,
		)}
	default:
		return &types.BadRequestError{Message: fmt.Sprintf(
			"LastEventID %d must be a DecisionTaskStarted, DecisionTaskTimedOut or DecisionTaskFailed event, got: %v",
			event.ID, event.GetEventType(),
		)}
	}
}
