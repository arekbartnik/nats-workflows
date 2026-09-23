package workflows

import (
	"context"
	"fmt"

	"github.com/arekbartnik/nats-workflows/engine"
)

type OrderState struct {
	Order       Order  `json:"order"`
	Reservation string `json:"reservation,omitempty"`
	Fraud       string `json:"fraud,omitempty"` // low | high
	FraudReview string `json:"fraud_review,omitempty"`
	Charge      string `json:"charge,omitempty"`
	Invoice     string `json:"invoice,omitempty"`
	Label       string `json:"label,omitempty"`
	Warehouse   string `json:"warehouse,omitempty"`
	Picked      string `json:"picked,omitempty"`
	Email       string `json:"email,omitempty"`
	ReviewReq   string `json:"review_request,omitempty"`
	Result      string `json:"result,omitempty"`
}

// Amounts use whole currency units in this demo.
type Order struct {
	Item   string `json:"item"`
	Qty    int    `json:"qty"`
	Amount int    `json:"amount"`
}

// External calls are simulated. Real integrations need stable idempotency
// keys because a crash can redeliver a Step after its external effect.
// Separate Hooks prevent late approvals from reaching a later wait.
func OrderPipeline() engine.Workflow {
	return engine.Workflow{
		Name: "order",
		Steps: []engine.Step{
			{Name: "validate-order", Run: engine.Typed(validateOrder)},
			{Name: "reserve-inventory", Run: engine.Typed(reserveInventory)},
			{Name: "fraud-score", Run: engine.Typed(fraudScore)},
			{Name: "fraud-review", Input: decisionContract, Run: engine.Typed(fraudOK)},
			{Name: "charge-payment", Run: engine.Typed(chargePayment)},
			{Name: "generate-invoice", Run: engine.Typed(generateInvoice)},
			{Name: "request-shipping-label", Run: engine.Typed(shippingLabel)},
			{Name: "prepare-warehouse", Run: engine.Typed(prepareWarehouse)},
			{Name: "confirm-picked", Input: decisionContract, Run: engine.Typed(picked)},
			{Name: "send-confirmation-email", Run: engine.Typed(confirmEmail)},
			{Name: "schedule-review-request", Run: engine.Typed(reviewRequest)},
			{Name: "close-order", Run: engine.Typed(closeOrder)},
		},
		Result: func(run *engine.Run) string {
			s, _ := engine.StateOf[OrderState](run)
			return s.Result
		},
	}
}

func validateOrder(ctx context.Context, run *engine.Run, s *OrderState) error {
	o := s.Order
	if o.Item == "" || o.Qty < 1 || o.Amount < 1 {
		return fmt.Errorf("%w: invalid order: %+v", engine.ErrTerminal, o)
	}
	return nil
}

func reserveInventory(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Reservation = "res-" + run.ID[:8]
	return nil
}

func fraudScore(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Fraud = "low"
	if s.Order.Amount >= 500 {
		s.Fraud = "high"
	}
	return nil
}

func fraudOK(ctx context.Context, run *engine.Run, s *OrderState) error {
	if s.Fraud == "low" {
		s.FraudReview = "auto-approved (low score)"
		return nil
	}
	input, err := engine.HookInput[DecisionInput](ctx, run, "fraud-review", "")
	if err != nil {
		return err
	}
	switch input.Decision {
	case "approve":
		s.FraudReview = "approved by reviewer"
		return nil
	case "reject":
		return fmt.Errorf("%w: order rejected by fraud review", engine.ErrTerminal)
	default:
		return fmt.Errorf("%w: fraud review needs a decision (approve|reject)", engine.ErrPark)
	}
}

func chargePayment(ctx context.Context, run *engine.Run, s *OrderState) error {
	// Use run.ID as the payment idempotency key to prevent duplicate charges.
	s.Charge = "ch-" + run.ID[:8]
	return nil
}

func generateInvoice(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Invoice = "inv-" + run.ID[:8]
	return nil
}

func shippingLabel(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Label = "label-" + run.ID[:8]
	return nil
}

func prepareWarehouse(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Warehouse = "ready for notification"
	return nil
}

func picked(ctx context.Context, run *engine.Run, s *OrderState) error {
	// Register before notification so an immediate warehouse callback can
	// submit using this token. A real notification uses run.ID as its
	// idempotency key because this Step may execute more than once.
	if _, err := run.RegisterHook(ctx, "confirm-picked", ""); err != nil {
		return err
	}
	s.Warehouse = "notified"
	input, err := engine.HookInput[DecisionInput](ctx, run, "confirm-picked", "")
	if err != nil {
		return err
	}
	if input.Decision != "approve" {
		return fmt.Errorf("%w: awaiting pick confirmation from the warehouse", engine.ErrTerminal)
	}
	s.Picked = "pick confirmed by warehouse"
	return nil
}

func confirmEmail(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Email = "confirmation sent"
	return nil
}

func reviewRequest(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.ReviewReq = "scheduled in 14 days"
	return nil
}

func closeOrder(ctx context.Context, run *engine.Run, s *OrderState) error {
	s.Result = fmt.Sprintf(
		"order closed: %d× %s (%d), fraud %s, charge %s, invoice %s, label %s, %s, %s, review %s",
		s.Order.Qty, s.Order.Item, s.Order.Amount, s.FraudReview, s.Charge,
		s.Invoice, s.Label, s.Picked, s.Email, s.ReviewReq)
	return nil
}

type DecisionInput struct {
	Decision string `json:"decision"`
}

var decisionContract = engine.HookType[DecisionInput]("decision-v1", func(input DecisionInput) error {
	if input.Decision != "approve" && input.Decision != "reject" {
		return fmt.Errorf("decision must be approve or reject")
	}
	return nil
})
