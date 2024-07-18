import torch
import numpy as np

class GMM:
    def __init__(self, n_components):
        self.n_components = n_components
        self.weights = torch.ones(n_components) / n_components
        self.means = torch.randn(n_components, 3)
        self.covariances = torch.zeros(n_components, 3, 3)

    def fit(self, X, max_iters=100, tol=1e-4):
        n_samples = X.shape[0]
        n_features = X.shape[1]

        for iteration in range(max_iters):
            responsibilities = self._e_step(X)
            self._m_step(X, responsibilities)
            if self._is_converged(X, responsibilities, tol):
                break

    def _e_step(self, X):
        responsibilities = torch.zeros(self.n_components, X.shape[0])
        for i in range(self.n_components):
            mean = self.means[i]
            covariance = self.covariances[i]
            det = torch.det(covariance)
            inv_covariance = self._inverse(covariance)
            for j in range(X.shape[0]):
                x = X[j]
                diff = x - mean
                exponent = -0.5 * torch.mm(torch.mm(diff.view(1, -1), inv_covariance), diff.view(-1, 1))
                responsibilities[i, j] = (1.0 / torch.sqrt((2 * np.pi) ** X.shape[1] * det)) * torch.exp(exponent)
        responsibilities = responsibilities * self.weights.view(-1, 1)
        responsibilities = responsibilities / responsibilities.sum(0)
        return responsibilities

    def _m_step(self, X, responsibilities):
        Nk = responsibilities.sum(1)
        self.weights = Nk / X.shape[0]
        for i in range(self.n_components):
            self.means[i] = torch.sum(responsibilities[i].view(-1, 1) * X, dim=0) / Nk[i]
            diff = X - self.means[i]
            self.covariances[i] = torch.mm((responsibilities[i].view(1, -1) * diff.T), diff) / Nk[i]

    def _inverse(self, matrix):
        return torch.inverse(matrix + torch.eye(matrix.shape[0]) * 1e-6)

    def _is_converged(self, X, responsibilities, tol):
        prev_log_likelihood = self._log_likelihood(X, responsibilities)
        responsibilities = self._e_step(X)
        current_log_likelihood = self._log_likelihood(X, responsibilities)
        return abs(current_log_likelihood - prev_log_likelihood) < tol

    def _log_likelihood(self, X, responsibilities):
        log_likelihood = torch.log(responsibilities.sum(0)).sum()
        return log_likelihood

    def predict(self, X):
        responsibilities = self._e_step(X)
        labels = torch.argmax(responsibilities, dim=0)
        return labels

    def get_cluster_means(self):
        return self.means

    def get_cluster_covariances(self):
        return self.covariances
