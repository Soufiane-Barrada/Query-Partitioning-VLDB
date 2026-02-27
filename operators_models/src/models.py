from __future__ import annotations
import numpy as np
from typing import Dict, Any
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import make_scorer

from xgboost import XGBRegressor

from lightgbm import LGBMRegressor

from catboost import CatBoostRegressor


from .config import MODEL_FACTORIES, PARAM_SPACES


def _softplus(x):
    # numerically-stable softplus : log(1 + exp(-|x|)) + max(x, 0)
    return np.log1p(np.exp(-np.abs(x))) + np.maximum(x, 0.0)

def _sigmoid(x):
    # numerically-stable sigmoid
    out = np.empty_like(x, dtype=float)
    pos = x >= 0
    neg = ~pos
    out[pos] = 1.0 / (1.0 + np.exp(-x[pos]))
    expx = np.exp(x[neg])
    out[neg] = expx / (1.0 + expx)
    return out

def lgbm_qloss_postprocess_pred(raw_pred):
    # apply the same link at inference time
    return _softplus(np.asarray(raw_pred, float))


# Q error / Q loss

def q_error(y_true, y_pred, eps=1e-6):
    y_true = np.clip(np.asarray(y_true, float), eps, None)
    y_pred = np.clip(np.asarray(y_pred, float), eps, None)
    return np.maximum(y_pred / y_true, y_true / y_pred)

def q_loss(y_true, y_pred, min_value=1e-9, eps=1e-6, penalty_negative=1e4):
    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)
    loss = np.empty_like(y_pred, dtype=float)
    mask = y_pred >= min_value
    if np.any(mask):
        ratio = (y_pred[mask] + eps) / (y_true[mask] + eps)
        loss[mask] = np.abs(np.log(ratio))
    if np.any(~mask):
        # penalize only negatives; small positives are handled by the log-ratio term
        loss[~mask] = penalty_negative * (1.0 - np.clip(y_pred[~mask], -1e6, 0.0))
    return loss



# shared grad/hess helper for QLoss
def _qloss_grad_hess(p: np.ndarray, t: np.ndarray):
    eps, min_value, penalty_negative = 1e-6, 1e-9, 1e4
    p = np.asarray(p, float)
    t = np.asarray(t, float)

    grad = np.zeros_like(p, float)
    hess = np.zeros_like(p, float)

    mask = p >= min_value
    if np.any(mask):
        ratio = (p[mask] + eps) / (t[mask] + eps)
        log_ratio = np.log(ratio)
        grad[mask] = np.sign(log_ratio) / (p[mask] + eps)
        hess[mask] = 1.0 / ((p[mask] + eps) ** 2 + 1e-12)
    if np.any(~mask):
        grad[~mask] = -penalty_negative
        hess[~mask] = 1e-6
    return grad, hess








# ---- XGBoost
def qloss_objective_xgb_ytrue(y_true: np.ndarray, y_pred: np.ndarray):
    # xgboost.sklearn paths call (y_true, y_pred)
    return _qloss_grad_hess(y_pred, y_true)

def qloss_objective_xgb_dtrain(y_pred: np.ndarray, dtrain):
    # xgboost.train calls (y_pred, dtrain)
    return _qloss_grad_hess(y_pred, dtrain.get_label().astype(float))

def qloss_objective(*args):
    if len(args) == 2 and hasattr(args[1], "get_label"):
        return qloss_objective_xgb_dtrain(args[0], args[1])
    elif len(args) == 2:
        return qloss_objective_xgb_ytrue(args[0], args[1])
    raise TypeError("Unsupported objective call signature for qloss_objective")





# ---- LightGBM
def qloss_objective_lgbm(labels, preds, weight=None, group=None):
    """
    Custom QLoss for LightGBM with a positivity link:
      z = raw score,  p = softplus(z)
      L = | log( (p+eps)/(t+eps) ) |
    Returns grad = dL/dz and hess ≈ (Gauss–Newton) >= 0.
    """
    eps = 1e-6

    y_true = np.asarray(labels, float)
    z      = np.asarray(preds,  float)

    p  = _softplus(z)                 # p > 0
    s  = _sigmoid(z)                  # dp/dz
    lr = np.log((p + eps) / (y_true + eps))  # log-ratio

    # dL/dp = sign(lr)/p
    sign_lr = np.sign(lr)
    dL_dp   = sign_lr / (p + eps)

    # Chain rule to raw score z
    grad = dL_dp * s

    # Gauss–Newton Hessian: (dp/dz)^2 * (dL/dp')' ≈ (s/p)^2
    hess = (s / (p + eps)) ** 2

    if weight is not None:
        w = np.asarray(weight, float)
        grad *= w
        hess *= w

    # tiny floor to avoid degenerate splits
    hess = np.maximum(hess, 1e-12)
    return grad, hess





def qloss_scorer():
    def _score(y_true, y_pred): return float(np.mean(q_loss(y_true, y_pred)))
    return make_scorer(_score, greater_is_better=False)




# param spaces (used for random search) 

def xgb_param_space():
    return {
        "n_estimators": [300, 600, 900, 1200, 1500, 2000],
        "learning_rate": np.logspace(-3, 0, 10),
        "max_depth": [3,4,5,6,7,8,9],
        "min_child_weight": [1,2,3,5,7],
        "subsample": [0.6, 0.8, 1.0],
        "colsample_bytree": [0.6, 0.8, 1.0],
        "reg_alpha": np.logspace(-5, 1, 7),
        "reg_lambda": np.logspace(-3, 2, 6),
        "gamma": [0, 0.1, 0.2, 0.5, 1.0],
    }
PARAM_SPACES.update({"xgb": xgb_param_space})




# factories

def build_xgb(objective="qloss", seed=42, **overrides):
    if objective == "mse":
        obj = "reg:squarederror"
    elif objective == "huber":
        # built-in pseudo Huber in XGBoost
        obj = "reg:pseudohubererror"
    else:
        obj = qloss_objective
    base = dict(objective=obj, tree_method="hist", random_state=seed, n_jobs=-1)
    base.update(overrides)
    return XGBRegressor(**base)

def build_lgbm(objective="mse", seed=42, **overrides):

    if objective == "qloss":
        obj = qloss_objective_lgbm
        metric = "None"
    elif objective == "huber":
        obj = "huber"; metric = None
    else:
        obj = "regression"; metric = None

    base = dict(objective=obj, random_state=seed, n_jobs=-1)
    if metric is not None:
        base["metric"] = metric
    base.update(overrides)
    return LGBMRegressor(**base)




def build_cat(objective="mse", seed=42, **overrides):
    if objective == "qloss":
        raise ValueError("QLoss is only supported with --model xgb.")
    loss_fn = "Huber:delta=1.0" if objective == "huber" else "RMSE"
    base = dict(loss_function=loss_fn, random_seed=seed, verbose=False)
    base.update(overrides)
    return CatBoostRegressor(**base)

MODEL_FACTORIES.update({
    "xgb":  build_xgb,
    "lgbm": build_lgbm,
    "cat":  build_cat,
})

#  simple random search
def random_search(model, param_space, X, y, n_iter=30, seed=42):
    search = RandomizedSearchCV(
        estimator=model,
        param_distributions=param_space,
        n_iter=n_iter,
        scoring=qloss_scorer(),
        cv=5,
        verbose=1,
        n_jobs=-1,
        random_state=seed,
    )
    search.fit(X, y)
    best = model.set_params(**search.best_params_)
    return best, search
