import { Button } from '@components/common/button';
import Home from '@components/home';
import React from 'react';
import { BrowserRouter as Router, Link, Route, Switch } from 'react-router-dom';

export interface IAppRouterProps {}

class AppRouter extends React.Component<IAppRouterProps> {
  public render() {
    return (
      <Router>
        <div>
          <Switch>
            <Route path="/" component={Home} />
          </Switch>
        </div>
      </Router>
    );
  }
}

export default AppRouter;
